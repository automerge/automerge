use std::collections::HashMap;

use automerge::{transaction::Transactable, ObjType};
use automerge_test::{assert_doc, list, map};
use rand::{prelude::IteratorRandom, Rng, SeedableRng};

trait NodeBehaviour {
    fn on_receive(
        &self,
        node: NodeId,
        doc: &mut automerge::AutoCommit,
        peers: &mut Peers,
        from: NodeId,
    ) -> Vec<Envelope>;

    fn on_change(
        &self,
        node: NodeId,
        doc: &mut automerge::AutoCommit,
        peers: &mut Peers,
    ) -> Vec<Envelope>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct NodeId(uuid::Uuid);

impl NodeId {
    fn new<R: rand::Rng>(rng: &mut R) -> Self {
        let random_bytes = rng.gen();
        let uid = uuid::Builder::from_random_bytes(random_bytes).into_uuid();
        Self(uid)
    }
}

#[derive(Clone, Debug)]
enum Message {
    Sync(automerge::sync::Message),
}

impl From<automerge::sync::Message> for Message {
    fn from(m: automerge::sync::Message) -> Self {
        Self::Sync(m)
    }
}

#[derive(Clone, Debug)]
struct Envelope {
    from: NodeId,
    to: NodeId,
    message: Message,
}

#[derive(Debug)]
struct Channel {
    a: NodeId,
    b: NodeId,
    a_to_b: Vec<Message>,
    b_to_a: Vec<Message>,
}

impl Channel {
    fn new(a: NodeId, b: NodeId) -> Self {
        Channel {
            a,
            b,
            a_to_b: Vec::new(),
            b_to_a: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.a_to_b.is_empty() && self.b_to_a.is_empty()
    }

    fn take_message<R: rand::Rng>(&mut self, rng: &mut R) -> Option<Envelope> {
        if (!self.a_to_b.is_empty()) && (!self.b_to_a.is_empty()) {
            let a: bool = rng.gen_bool(0.5);
            if a {
                self.a_to_b.pop().map(|m| Envelope {
                    from: self.a,
                    to: self.b,
                    message: m,
                })
            } else {
                self.b_to_a.pop().map(|m| Envelope {
                    from: self.b,
                    to: self.a,
                    message: m,
                })
            }
        } else if !self.a_to_b.is_empty() {
            self.a_to_b.pop().map(|m| Envelope {
                from: self.a,
                to: self.b,
                message: m,
            })
        } else {
            self.b_to_a.pop().map(|m| Envelope {
                from: self.b,
                to: self.a,
                message: m,
            })
        }
    }

    fn enqueue(&mut self, from: NodeId, to: NodeId, msg: Message) {
        assert!(from == self.a || from == self.b);
        assert!(to == self.a || to == self.b);

        if from == self.a {
            self.a_to_b.push(msg);
        } else {
            self.b_to_a.push(msg);
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, Eq, Ord, PartialOrd, PartialEq)]
struct ChannelKey(NodeId, NodeId);

impl From<(NodeId, NodeId)> for ChannelKey {
    fn from((a, b): (NodeId, NodeId)) -> Self {
        if a > b {
            Self(a, b)
        } else {
            Self(b, a)
        }
    }
}

#[derive(Debug)]
struct InFlight(HashMap<ChannelKey, Channel>);

impl InFlight {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn is_empty(&self) -> bool {
        self.0.values().all(|c| c.is_empty())
    }

    fn take_random_message<R: rand::Rng>(&mut self, rng: &mut R) -> Option<Envelope> {
        if self.is_empty() {
            None
        } else {
            loop {
                let channel = self.0.values_mut().choose(rng).unwrap();
                match channel.take_message(rng) {
                    Some(m) => return Some(m),
                    None => continue,
                }
            }
        }
    }

    fn enqueue(
        &mut self,
        Envelope {
            from,
            to,
            message: msg,
        }: Envelope,
    ) {
        let key = ChannelKey::from((from, to));
        let channel = if let Some(channel) = self.0.get_mut(&key) {
            channel
        } else {
            let channel = Channel::new(from, to);
            self.0.insert(key, channel);
            self.0.get_mut(&key).unwrap()
        };
        channel.enqueue(from, to, msg);
    }
}

struct Peers(HashMap<NodeId, automerge::sync::State>);

impl Peers {
    fn new() -> Self {
        Self(Default::default())
    }

    fn sync_message_for(
        &mut self,
        other: NodeId,
        doc: &mut automerge::AutoCommit,
    ) -> Option<Message> {
        let state = if let Some(state) = self.0.get_mut(&other) {
            state
        } else {
            let state = automerge::sync::State::new();
            self.0.insert(other, state);
            self.0.get_mut(&other).unwrap()
        };
        doc.generate_sync_message(state).map(|m| m.into())
    }

    fn sync_message_for_all(&mut self, doc: &mut automerge::AutoCommit) -> Vec<(NodeId, Message)> {
        self.0
            .iter_mut()
            .filter_map(|(node, state)| doc.generate_sync_message(state).map(|m| (*node, m.into())))
            .collect()
    }

    fn receive_sync_message(
        &mut self,
        from: NodeId,
        msg: automerge::sync::Message,
        doc: &mut automerge::AutoCommit,
    ) -> Result<(), automerge::AutomergeError> {
        let state = if let Some(state) = self.0.get_mut(&from) {
            state
        } else {
            let state = automerge::sync::State::new();
            self.0.insert(from, state);
            self.0.get_mut(&from).unwrap()
        };
        doc.receive_sync_message(state, msg)
    }
}

struct Node {
    name: String,
    id: NodeId,
    doc: automerge::AutoCommit,
    peers: Peers,
    behaviour: Box<dyn NodeBehaviour>,
}

impl Node {
    fn new<R: rand::Rng, B: NodeBehaviour + 'static>(
        rng: &mut R,
        behaviour: B,
        name: String,
    ) -> Self {
        let random_bytes: [u8; 16] = rng.gen();
        let actor_id: automerge::ActorId = random_bytes.into();
        Self {
            name,
            id: NodeId::new(rng),
            doc: automerge::AutoCommit::new().with_actor(actor_id),
            peers: Peers::new(),
            behaviour: Box::new(behaviour),
        }
    }

    fn receive(&mut self, msg: Envelope) -> Result<Vec<Envelope>, automerge::AutomergeError> {
        let Envelope {
            from, message: msg, ..
        } = msg;
        let Message::Sync(m) = msg;
        self.peers.receive_sync_message(from, m, &mut self.doc)?;
        Ok(self
            .behaviour
            .on_receive(self.id, &mut self.doc, &mut self.peers, from))
    }

    fn change<F, O, E>(&mut self, f: F) -> Result<(Vec<Envelope>, O), E>
    where
        F: Fn(&mut automerge::AutoCommit) -> Result<O, E>,
    {
        let result = f(&mut self.doc)?;
        //let mut msgs = Vec::with_capacity(self.sync_states.len());
        let msgs = self
            .behaviour
            .on_change(self.id, &mut self.doc, &mut self.peers);
        Ok((msgs, result))
    }

    fn sync_with(&mut self, other: NodeId) -> Option<Message> {
        self.peers.sync_message_for(other, &mut self.doc)
    }
}

#[derive(Debug)]
enum StepError {
    NeverBecameQuiet,
    ErrorDelivering {
        from: NodeId,
        to: NodeId,
        error: automerge::AutomergeError,
    },
}

impl std::error::Error for StepError {}

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeverBecameQuiet => write!(f, "the network never became quiet"),
            Self::ErrorDelivering { from, to, error } => {
                write!(
                    f,
                    "Error delivering message from {:?} to {:?}: {:?}",
                    from, to, error,
                )
            }
        }
    }
}

#[derive(Debug)]
enum Event {
    Change(NodeId),
    Send(Envelope),
    Deliver(Envelope),
}

struct Simulation {
    time: u64,
    seed: u64,
    rng: rand::rngs::SmallRng,
    nodes: Vec<Node>,
    in_flight: InFlight,
    events: Vec<Event>,
}

struct Events {
    node_names: HashMap<NodeId, String>,
    events: Vec<Event>,
}

impl Events {
    fn new<'a, I: Iterator<Item = &'a Node>>(nodes: I, events: Vec<Event>) -> Self {
        let node_names: HashMap<NodeId, String> =
            nodes.map(|node| (node.id, node.name.clone())).collect();
        Self { node_names, events }
    }
}

impl std::fmt::Display for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.events.is_empty() {
            write!(f, "[]")
        } else {
            writeln!(f, "[")?;
            for event in &self.events {
                match event {
                    Event::Change(id) => writeln!(f, "\tChange on {}", self.node_names[id])?,
                    Event::Send(_env) => {}
                    Event::Deliver(env) => writeln!(
                        f,
                        "\tdeliver {} ---> {}",
                        self.node_names[&env.from], self.node_names[&env.to]
                    )?,
                }
            }
            writeln!(f, "]")
        }
    }
}

struct SimulationBuilder {
    seed: u64,
    rng: rand::rngs::SmallRng,
    nodes: Vec<Node>,
}

impl SimulationBuilder {
    fn from_seed(seed: u64) -> Self {
        Self {
            seed,
            rng: rand::rngs::SmallRng::seed_from_u64(seed),
            nodes: Vec::new(),
        }
    }

    fn new() -> Self {
        let seed: u64 = rand::thread_rng().gen();
        Self::from_seed(seed)
    }

    fn add_node<F: Fn(&mut rand::rngs::SmallRng) -> Node>(&mut self, f: F) -> NodeId {
        let node = f(&mut self.rng);
        let id = node.id;
        self.nodes.push(node);
        id
    }

    fn build(self) -> Simulation {
        assert!(!self.nodes.is_empty());
        Simulation::new(self.seed, self.rng, self.nodes)
    }
}

impl Simulation {
    fn new(seed: u64, rng: rand::rngs::SmallRng, nodes: Vec<Node>) -> Self {
        Self {
            seed,
            rng,
            nodes,
            in_flight: InFlight::new(),
            events: Vec::new(),
            time: 0,
        }
    }

    fn change_on_node<F: Fn(&mut automerge::AutoCommit) -> Result<O, E>, O, E>(
        &mut self,
        node: &NodeId,
        f: F,
    ) -> Result<O, E> {
        let node = self.nodes.iter_mut().find(|p| &p.id == node).unwrap();
        let (msgs, result) = node.change(f)?;
        self.events.push(Event::Change(node.id));
        for msg in msgs {
            self.send(msg);
        }
        Ok(result)
    }

    fn connect(&mut self, from: NodeId, to: NodeId) {
        let from = self.nodes.iter_mut().find(|p| p.id == from).unwrap();
        if let Some(message) = from.sync_with(to) {
            let from_id = from.id;
            self.send(Envelope {
                from: from_id,
                to,
                message,
            });
        }
    }

    fn step(&mut self) -> Result<bool, StepError> {
        self.time += 1;
        match self.in_flight.take_random_message(&mut self.rng) {
            Some(env) => {
                self.events.push(Event::Deliver(env.clone()));
                let node = self.nodes.iter_mut().find(|n| n.id == env.to).unwrap();
                let new_msgs =
                    node.receive(env.clone())
                        .map_err(|e| StepError::ErrorDelivering {
                            from: env.from,
                            to: env.to,
                            error: e,
                        })?;
                for message in new_msgs {
                    self.send(message);
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn send(&mut self, msg: Envelope) {
        self.events.push(Event::Send(msg.clone()));
        self.in_flight.enqueue(msg);
    }

    /// Run until there are no messages left to deliver. Returns the number of iterations
    fn run_until_quiet(&mut self, timeout_steps: u64) -> Result<u64, StepError> {
        let mut num_iterations = 0;
        while num_iterations < timeout_steps {
            num_iterations += 1;
            if !self.step()? {
                return Ok(num_iterations);
            }
        }
        Err(StepError::NeverBecameQuiet)
    }

    fn take_events(&mut self) -> Events {
        Events::new(self.nodes.iter(), std::mem::take(&mut self.events))
    }
}

struct SyncClient {
    sync_server: NodeId,
}

impl NodeBehaviour for SyncClient {
    fn on_change(
        &self,
        node: NodeId,
        doc: &mut automerge::AutoCommit,
        peers: &mut Peers,
    ) -> Vec<Envelope> {
        if let Some(message) = peers.sync_message_for(self.sync_server, doc) {
            vec![Envelope {
                from: node,
                to: self.sync_server,
                message,
            }]
        } else {
            Vec::new()
        }
    }

    fn on_receive(
        &self,
        _node: NodeId,
        _doc: &mut automerge::AutoCommit,
        _peers: &mut Peers,
        _from: NodeId,
    ) -> Vec<Envelope> {
        Vec::new()
    }
}

struct SyncServer;
impl NodeBehaviour for SyncServer {
    fn on_change(
        &self,
        node: NodeId,
        doc: &mut automerge::AutoCommit,
        peers: &mut Peers,
    ) -> Vec<Envelope> {
        peers
            .sync_message_for_all(doc)
            .into_iter()
            .map(|(to, message)| Envelope {
                from: node,
                to,
                message,
            })
            .collect()
    }

    fn on_receive(
        &self,
        node: NodeId,
        doc: &mut automerge::AutoCommit,
        peers: &mut Peers,
        _from: NodeId,
    ) -> Vec<Envelope> {
        peers
            .sync_message_for_all(doc)
            .into_iter()
            .map(|(to, message)| Envelope {
                from: node,
                to,
                message,
            })
            .collect()
    }
}

#[test]
fn sync_server_cluster() {
    let mut builder = SimulationBuilder::new();
    let server = builder.add_node(|rng| Node::new(rng, SyncServer, "sync-server".to_string()));
    let client1 = builder.add_node(|rng| {
        Node::new(
            rng,
            SyncClient {
                sync_server: server,
            },
            "client1".to_string(),
        )
    });
    let client2 = builder.add_node(|rng| {
        Node::new(
            rng,
            SyncClient {
                sync_server: server,
            },
            "client2".to_string(),
        )
    });
    let mut sim = builder.build();

    sim.connect(client1, server);
    sim.connect(client2, server);
    sim.run_until_quiet(1000).unwrap();

    let text = sim
        .change_on_node::<_, _, automerge::AutomergeError>(&client1, |doc| {
            doc.put_object(&automerge::ROOT, "text", ObjType::Text)
        })
        .unwrap();

    sim.run_until_quiet(1000).unwrap();
    sim.take_events();

    sim.change_on_node::<_, _, automerge::AutomergeError>(&client1, |doc| {
        doc.splice_text(&text, 0, 0, "ab")?;
        Ok(())
    })
    .unwrap();

    sim.run_until_quiet(1000).unwrap();

    assert!(sim.in_flight.is_empty());

    let messages_sent = sim
        .events
        .iter()
        .filter_map(|e| match e {
            Event::Send(..) => Some(()),
            _ => None,
        })
        .count();

    println!("Seed: {}", sim.seed);
    println!("Done after: {}", sim.time);
    println!("Events: {}", sim.take_events());

    assert_eq!(messages_sent, 3);

    for node in sim.nodes.iter_mut() {
        assert_doc!(
            node.doc.document(),
            map! {
                "text" => { list! {
                    {"a"},
                    {"b"},
                }}
            }
        )
    }
}

struct FullMesh;
impl NodeBehaviour for FullMesh {
    fn on_change(
        &self,
        node: NodeId,
        doc: &mut automerge::AutoCommit,
        peers: &mut Peers,
    ) -> Vec<Envelope> {
        peers
            .sync_message_for_all(doc)
            .into_iter()
            .map(|(to, msg)| Envelope {
                to,
                from: node,
                message: msg,
            })
            .collect()
    }

    fn on_receive(
        &self,
        node: NodeId,
        doc: &mut automerge::AutoCommit,
        peers: &mut Peers,
        _from: NodeId,
    ) -> Vec<Envelope> {
        peers
            .sync_message_for_all(doc)
            .into_iter()
            .map(|(to, msg)| Envelope {
                to,
                from: node,
                message: msg,
            })
            .collect()
    }
}

#[test]
fn full_mesh_cluster() {
    let mut builder = SimulationBuilder::new();
    let node1 = builder.add_node(|rng| Node::new(rng, FullMesh, "node1".to_string()));
    let node2 = builder.add_node(|rng| Node::new(rng, FullMesh, "node2".to_string()));
    let node3 = builder.add_node(|rng| Node::new(rng, FullMesh, "node3".to_string()));
    let mut sim = builder.build();

    sim.connect(node1, node2);
    sim.connect(node1, node3);
    sim.connect(node2, node3);

    sim.run_until_quiet(1000).unwrap();

    let text = sim
        .change_on_node::<_, _, automerge::AutomergeError>(&node1, |doc| {
            doc.put_object(&automerge::ROOT, "text", ObjType::Text)
        })
        .unwrap();

    sim.run_until_quiet(1000).unwrap();
    sim.take_events();

    sim.change_on_node::<_, _, automerge::AutomergeError>(&node2, |doc| {
        doc.splice_text(&text, 0, 0, "ab")?;
        Ok(())
    })
    .unwrap();

    sim.run_until_quiet(1000).unwrap();

    assert!(sim.in_flight.is_empty());

    let _messages_sent = sim
        .events
        .iter()
        .filter_map(|e| match e {
            Event::Send(..) => Some(()),
            _ => None,
        })
        .count();

    println!("Seed: {}", sim.seed);
    println!("Done after: {}", sim.time);
    println!("Events: {}", sim.take_events());

    for node in sim.nodes.iter_mut() {
        assert_doc!(
            node.doc.document(),
            map! {
                "text" => { list! {
                    {"a"},
                    {"b"},
                }}
            }
        )
    }
}
