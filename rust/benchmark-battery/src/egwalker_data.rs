use anyhow::{bail, Context, Result};
use automerge::{
    transaction::Transactable, ActorId, AutoCommit, ObjType, ReadDoc, TextEncoding, ROOT,
};
use serde::Deserialize;
use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

const ROOT_BRANCH: usize = usize::MAX;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EditTrace {
    end_content: String,
    num_agents: usize,
    txns: Vec<Transaction>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Transaction {
    parents: Vec<usize>,
    num_children: usize,
    agent: usize,
    patches: Vec<TextPatch>,
}

#[derive(Debug, Deserialize)]
struct TextPatch(usize, usize, String);

type Branches = HashMap<usize, (AutoCommit, usize)>;

pub fn generate(inputs: &[impl AsRef<Path>]) -> Result<()> {
    if inputs.is_empty() {
        bail!("at least one JSON trace is required");
    }

    for input in inputs {
        convert(input.as_ref())?;
    }
    Ok(())
}

fn convert(input: &Path) -> Result<()> {
    let file = File::open(input).with_context(|| format!("opening {}", input.display()))?;
    let trace: EditTrace = serde_json::from_reader(BufReader::new(file))
        .with_context(|| format!("parsing {}", input.display()))?;
    let root_count = validate(&trace).with_context(|| format!("validating {}", input.display()))?;

    eprintln!(
        "Converting {} ({} transactions)",
        input.display(),
        trace.txns.len()
    );

    let mut root = AutoCommit::new_with_encoding(TextEncoding::UnicodeCodePoint);
    root.set_actor(ActorId::from(&[0xff][..]));
    let text = root
        .put_object(ROOT, "text", ObjType::Text)
        .context("creating the text object")?;

    let mut branches = Branches::new();
    branches.insert(ROOT_BRANCH, (root, root_count));

    let total_patches: usize = trace.txns.iter().map(|txn| txn.patches.len()).sum();
    let progress_interval = (total_patches / 10).max(1);
    let mut applied_patches = 0;
    let mut next_progress = progress_interval;
    let mut result = None;

    for (index, txn) in trace.txns.iter().enumerate() {
        let (first_parent, other_parents) = txn
            .parents
            .split_first()
            .map_or((ROOT_BRANCH, &[][..]), |(first, rest)| (*first, rest));

        let mut doc = take_branch(&mut branches, first_parent, Some(txn.agent))?;
        for parent in other_parents {
            let mut other = take_branch(&mut branches, *parent, None)?;
            doc.merge(&mut other)
                .with_context(|| format!("merging parent {parent} of transaction {index}"))?;
        }

        // Trace positions count Unicode code points. The source data occasionally contains
        // positions beyond the current text, so preserve the original converter's clamping.
        let mut text_len = doc.length(&text);
        for TextPatch(position, delete_count, inserted) in &txn.patches {
            let start = (*position).min(text_len);
            let end = position.saturating_add(*delete_count).min(text_len);
            let deleted = end - start;
            doc.splice_text(
                &text,
                start,
                isize::try_from(deleted).context("delete length does not fit in isize")?,
                inserted,
            )
            .with_context(|| format!("applying a patch in transaction {index}"))?;
            text_len = text_len - deleted + inserted.chars().count();

            applied_patches += 1;
            if applied_patches >= next_progress && applied_patches < total_patches {
                eprintln!("  {applied_patches}/{total_patches} patches");
                next_progress += progress_interval;
            }
        }
        doc.commit();

        if txn.num_children == 0 {
            result = Some(doc);
        } else {
            branches.insert(index, (doc, txn.num_children));
        }
    }

    let mut doc = result.context("trace has no final transaction")?;
    let actual = doc.text(&text).context("reading the generated text")?;
    if actual != trace.end_content {
        eprintln!(
            "  warning: generated text differs from the trace's endContent (this is expected for some concurrent traces)"
        );
    }

    let output = input.with_extension("am");
    let bytes = doc.save();
    std::fs::write(&output, &bytes).with_context(|| format!("writing {}", output.display()))?;
    eprintln!("  wrote {} ({} bytes)", output.display(), bytes.len());
    Ok(())
}

fn validate(trace: &EditTrace) -> Result<usize> {
    if trace.txns.is_empty() {
        bail!("trace contains no transactions");
    }

    let mut actual_children = vec![0usize; trace.txns.len()];
    let mut root_count = 0;
    let mut final_count = 0;

    for (index, txn) in trace.txns.iter().enumerate() {
        if txn.agent >= trace.num_agents {
            bail!(
                "transaction {index} uses agent {}, but numAgents is {}",
                txn.agent,
                trace.num_agents
            );
        }
        if txn.parents.is_empty() {
            root_count += 1;
        }
        if txn.num_children == 0 {
            final_count += 1;
        }
        for parent in &txn.parents {
            if *parent >= index {
                bail!("transaction {index} has non-ancestor parent {parent}");
            }
            actual_children[*parent] += 1;
        }
    }

    if root_count == 0 {
        bail!("trace has no root transactions");
    }
    if final_count != 1 {
        bail!("trace must have one final transaction, found {final_count}");
    }
    for (index, (txn, actual)) in trace.txns.iter().zip(actual_children).enumerate() {
        if txn.num_children != actual {
            bail!(
                "transaction {index} declares {} children but has {actual}",
                txn.num_children
            );
        }
    }

    Ok(root_count)
}

fn take_branch(branches: &mut Branches, index: usize, agent: Option<usize>) -> Result<AutoCommit> {
    let (parent, remaining_uses) = branches
        .get_mut(&index)
        .with_context(|| format!("transaction refers to unavailable parent {index}"))?;

    let mut doc = if *remaining_uses == 1 {
        branches.remove(&index).expect("branch was just found").0
    } else {
        *remaining_uses -= 1;
        parent.fork()
    };

    if let Some(agent) = agent {
        // The paper's converter used the big-endian bytes of a 64-bit usize. Use u64
        // explicitly so actor IDs are deterministic on every host.
        doc.set_actor(ActorId::from((agent as u64).to_be_bytes().as_slice()));
    }
    Ok(doc)
}
