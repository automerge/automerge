use super::SampledBenchmark;
use benchmark_battery::automerge::{ActorId, AutoCommit, Change, ExpandedChange, ReadDoc, ROOT};
use std::hint::black_box;

pub(super) const FILES: [&str; 6] = ["S1.am", "S3.am", "A1.am", "A2.am", "C1.am", "C2.am"];

pub fn benchmarks() -> Vec<SampledBenchmark> {
    let mut benchmarks = Vec::new();
    for filename in FILES {
        let label = filename.trim_end_matches(".am");
        benchmarks.extend([
            SampledBenchmark::no_setup(
                "egwalker_paper",
                Box::leak(format!("egwalker_paper/load/{label}").into_boxed_str()),
                move || load(filename),
            ),
            SampledBenchmark::no_setup(
                "egwalker_paper",
                Box::leak(format!("egwalker_paper/get_text/{label}").into_boxed_str()),
                move || get_text(filename),
            ),
            SampledBenchmark::no_setup(
                "egwalker_paper",
                Box::leak(format!("egwalker_paper/clone/{label}").into_boxed_str()),
                move || clone_doc(filename),
            ),
            // Clone outside the timer, but measure the first fragment-boundary
            // query on that clone, including any cold-cache reconstruction.
            SampledBenchmark::batched(
                "egwalker_paper",
                Box::leak(
                    format!("egwalker_paper/apply_fragment_to_clone/{label}").into_boxed_str(),
                ),
                move || fragment_extension(filename),
                |(mut doc, change)| {
                    doc.apply_changes([change]).unwrap();
                    doc
                },
            ),
        ]);
    }
    benchmarks
}

fn load(filename: &str) -> Box<dyn FnMut()> {
    let bytes = read_file(filename);
    Box::new(move || {
        let doc = AutoCommit::load(&bytes).unwrap();
        black_box(doc);
    })
}

fn get_text(filename: &str) -> Box<dyn FnMut()> {
    let bytes = read_file(filename);
    let doc = AutoCommit::load(&bytes).unwrap();
    Box::new(move || {
        let (_, text_id) = doc.get(ROOT, "text").unwrap().unwrap();
        let result = doc.text(text_id).unwrap();
        black_box(result);
    })
}

fn clone_doc(filename: &str) -> Box<dyn FnMut()> {
    let doc = AutoCommit::load(&read_file(filename)).unwrap();
    Box::new(move || {
        black_box(doc.clone());
    })
}

fn fragment_extension(filename: &str) -> (AutoCommit, Change) {
    let doc = AutoCommit::load(&read_file(filename)).unwrap();
    let mut author = doc.clone();
    // Keep the imported change and actor-table insertion position identical
    // across revisions, instead of inheriting the load's random local actor.
    author.set_actor(ActorId::from(&[0xff; 32][..]));
    author.empty_change(Default::default());
    let mut expanded: ExpandedChange = (&author.get_last_local_change().unwrap()).into();
    // Hash mining is setup, not measured. Force exactly level 1 so this
    // benchmark always exercises boundary discovery instead of skipping it.
    for time in 0..1_000_000 {
        expanded.time = time;
        let change = Change::from(expanded.clone());
        let hash = change.hash();
        if hash.as_ref()[0] == 0 && hash.as_ref()[1] != 0 {
            return (doc, change);
        }
    }
    panic!("could not construct a level-1 change");
}

pub(super) fn read_file(filename: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("egwalker-paper")
            .join(filename),
    )
    .unwrap()
}
