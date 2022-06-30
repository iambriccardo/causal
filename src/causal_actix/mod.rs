use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Add;

use actix::{Actor, Addr, AsyncContext, Context, Handler, Recipient, System};
use actix::prelude::*;

use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState, SeqNr, VTime};
use crate::causal_impl::{Counter, InMemory};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;


/** TYPES **/

pub type ReplicasTable = HashMap<ReplicaId, Recipient<CausalMessage>>;

pub type CausalReceiver = Recipient<CausalMessage>;

pub enum ActixCommand {
    Increment
}


/** MESSAGES **/

#[derive(Message)]
#[rtype(result = "()")]
pub enum CausalMessage {
    // Message that represents the execution of a command in the receiving replica.
    Command(Option<CausalReceiver>, ActixCommand),
    // Message that represents the connection of the receiving replica to another.
    Connect(Option<CausalReceiver>, ReplicaId, CausalReceiver),
    // Message that represents a request to replicate the content of the receiving replica.
    Replicate(Option<CausalReceiver>, SeqNr, VTime)
}


/** ACTORS **/

pub struct Replica {
    id: ReplicaId,
    replica_state: Option<ReplicaState<Counter, u64, ActixCommand, u64>>,
    replicating_nodes: ReplicasTable,
    event_store: InMemory,
}

impl Replica {
    pub fn start_and_receive(id: ReplicaId) -> CausalReceiver {
        Replica {
            id,
            replica_state: None,
            replicating_nodes: HashMap::new(),
            event_store: InMemory {},
        }.start().recipient()
    }

    pub fn load_state(&mut self) {
        let mut state = self.event_store
            .load_snapshot()
            .or(Some(ReplicaState::create(self.id, Counter::default())))
            .unwrap();

        for event in self.event_store.load_events(state.seq_nr + 1) {
            state = state.process_event(&event);
        }

        self.replica_state = Some(state);
    }

    pub fn handle_command(&mut self, command: ActixCommand) {
        let state = self.replica_state
            .as_mut()
            .unwrap()
            .process_command(&command, &self.event_store);

        self.replica_state = Some(state);
    }

    pub fn handle_connect(&mut self, replica_id: ReplicaId, causal_receiver: CausalReceiver) {
        self.replicating_nodes.insert(replica_id, causal_receiver);
    }

    pub fn handle_replicate(&mut self, sender: CausalReceiver, seq_nr: SeqNr, version: VTime) {
        let events = self.replica_state
            .as_mut()
            .unwrap()
            .process_replay(seq_nr, version);
    }
}

impl Actor for Replica {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Replica {} started", self.id);
        println!("Loading state for replica {}...", self.id);
        self.load_state();
        println!("State loaded for replica {}", self.id);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        println!("Replica {} stopped", self.id);
    }
}

impl Handler<CausalMessage> for Replica {
    type Result = ();

    fn handle(&mut self, msg: CausalMessage, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            CausalMessage::Command(sender, command) => {
                println!("[COMMAND-{}]", self.id);
                self.handle_command(command);
            }
            CausalMessage::Connect(sender, replica_id, causal_receiver) => {
                println!("[CONNECT-{}]", self.id);
                self.handle_connect(replica_id, causal_receiver);
            },
            CausalMessage::Replicate(sender, seq_nr, version) => {
                println!("[REPLICATE-{}] with seq nr {} and version vector {}", self.id, seq_nr, version);
                self.handle_replicate(sender.unwrap(), seq_nr, version);
            },
            _ => {
                println!("Message is not supported.")
            }
        }
    }
}