use std::collections::{BTreeSet, HashMap, HashSet};

use crate::{change_graph::ChangeGraph, types::HASH_SIZE, Automerge, Bundle, Change, ChangeHash};

#[derive(Debug, Clone)]
pub struct Level(u8);

#[derive(Debug, Clone)]
pub struct TrailingBytesChunker(Level);

pub trait Chunker {
    type Next: Chunker;

    fn is_boundary(&self, change: &Change) -> bool;
    fn next_chunker(&self) -> Self::Next;
}

impl Chunker for TrailingBytesChunker {
    type Next = TrailingBytesChunker; // FIXME could be better!

    fn is_boundary(&self, change: &Change) -> bool {
        let bytes = change.hash().0;
        let idx = HASH_SIZE - (self.0 .0 as usize);
        bytes[idx..].iter().all(|&b| b == 0)
    }

    fn next_chunker(&self) -> Self::Next {
        TrailingBytesChunker(Level(self.0 .0 + 1))
    }
}

#[derive(Debug, Clone)]
pub struct Boundary(pub HashMap<ChangeHash, Change>);

#[derive(Debug, Clone)]
pub struct Chunky<T: Chunker> {
    chunker: T,
    automerge: Automerge,

    horizon: HashSet<ChangeHash>,
    boundary: HashMap<ChangeHash, Change>,

    visited: HashSet<ChangeHash>,
    members: HashSet<ChangeHash>,
}

impl<T: Chunker> Chunky<T> {
    pub fn new(automerge: Automerge, chunker: T) -> Self {
        Self {
            chunker,
            automerge,
            visited: HashSet::new(),
            horizon: HashSet::new(),
            boundary: HashMap::new(),
            members: HashSet::new(),
        }
    }

    // TODO run multiple levels, maybe trait needs a "fn next_level -> Level"?

    pub fn run(&mut self, head: &Change) -> (Boundary, Bundle) {
        // FIXME make horizon local only so taht no one can mess with it?
        // I guess the idiomatic rust way is to just tell people to not run something mid-state
        self.horizon.insert(head.hash());
        while !self.horizon.is_empty() {
            let horizon_hashes = self.horizon.iter().copied().collect::<Vec<_>>();
            let horizon_changes = self.automerge.get_changes(&horizon_hashes);
            if horizon_hashes.len() != horizon_changes.len() {
                panic!("Some changes in the horizon are missing from the automerge instance");
            }
            self.horizon.clear();

            // TODO tracing::
            for change in &horizon_changes {
                let digest = change.hash();
                self.horizon.remove(&digest);

                let is_newly_visited = self.visited.insert(digest);
                if !is_newly_visited {
                    continue;
                }

                self.members.insert(digest);

                if self.chunker.is_boundary(change) {
                    self.boundary.insert(digest, change.clone());
                } else {
                    let unexplored = change.deps().iter().filter(|&d| !self.visited.contains(d));
                    self.horizon.extend(unexplored);
                }
            }
        }

        // Cleanup

        let mut cleanup_horizon = self.boundary.keys().copied().collect::<Vec<_>>();
        while !cleanup_horizon.is_empty() {
            let cleanup_horizon_changes = self.automerge.get_changes(&cleanup_horizon);
            if cleanup_horizon.len() != cleanup_horizon_changes.len() {
                panic!("Some changes in the cleanup_horizon set are missing from the automerge instance");
            }
            cleanup_horizon.clear();

            for change in cleanup_horizon_changes {
                let change_hash = change.hash();

                self.members.remove(&change_hash);
                self.boundary.remove(&change_hash); // NOTE if one boundary covers another

                let is_newly_visited = self.visited.insert(change_hash);
                if !is_newly_visited {
                    continue;
                }

                let unexplored = change.deps().iter().filter(|&d| !self.visited.contains(d));
                cleanup_horizon.extend(unexplored);
            }
        }

        (
            Boundary(self.boundary.clone()),
            Bundle::for_hashes(todo!(), &self.automerge.change_graph, self.members).unwrap(), // FIXME handle error properly
        )
    }
}
