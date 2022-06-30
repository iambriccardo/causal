use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::ops::Add;

use actix::{Actor, Addr, AsyncContext, Context, Handler, Recipient, System};
use actix::prelude::*;

use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState, SeqNr, VTime};
use crate::causal_impl::{Counter, InMemory};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;
use crate::CausalMessage::{Command, Connect, Replicate, Replicated};

/** TYPES **/

pub type ReplicasTable<EVENT> = HashMap<ReplicaId, Recipient<CausalMessage<EVENT>>>;
pub type CausalReceiver<EVENT> = Recipient<CausalMessage<EVENT>>;

pub enum ActixCommand {
    Increment
}

impl Display for ActixCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut output = String::from("");

        match self {
            Increment => output.push_str("increment")
        }

        write!(f, "{}", output)
    }
}


/** MESSAGES **/
#[derive(Message)]
#[rtype(result = "()")]
pub enum CausalMessage<EVENT>
    where EVENT: Send + Clone
{
    // Message that represents the execution of a command in the receiving replica.
    Command(Option<CausalReceiver<EVENT>>, ActixCommand),
    // Message that represents the connection of the receiving replica to another.
    Connect(Option<CausalReceiver<EVENT>>, ReplicaId, CausalReceiver<EVENT>),
    // Message that represents a request to replicate the content of the receiving replica.
    Replicate(Option<CausalReceiver<EVENT>>, SeqNr, VTime),
    // Message that represents the replicated events that the receiving replica will apply locally.
    Replicated(Option<CausalReceiver<EVENT>>, SeqNr, Vec<Event<EVENT>>),
}


/** ACTORS **/

pub struct Replica {
    id: ReplicaId,
    // TODO: replace with generic CRDT.
    replica_state: Option<ReplicaState<Counter, u64, ActixCommand, u64>>,
    replicating_nodes: ReplicasTable<u64>,
    event_store: InMemory,
}

impl Replica {
    pub fn start_and_receive(id: ReplicaId) -> CausalReceiver<u64> {
        Replica {
            id,
            replica_state: None,
            replicating_nodes: HashMap::new(),
            event_store: InMemory::create(),
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
            .process_command(&command, &mut self.event_store);

        self.replica_state = Some(state);
    }

    pub fn handle_connect(&mut self, ctx: &mut <Replica as Actor>::Context, replica_id: ReplicaId, other_replica: CausalReceiver<u64>) {
        self.replicating_nodes.insert(replica_id, other_replica.clone());

        let (seq_nr, version) = self.replica_state
            .as_mut()
            .unwrap()
            .process_connect(replica_id);

        other_replica.do_send(
            Replicate(
                Some(ctx.address().recipient()),
                seq_nr,
                version,
            )
        ).expect("Error while sending [REPLICATE] request.")
    }

    pub fn handle_replicate(&mut self, ctx: &mut <Replica as Actor>::Context, sender: CausalReceiver<u64>, seq_nr: SeqNr, version: VTime) {
        let (last_seq_nr, events) = self.replica_state
            .as_mut()
            .unwrap()
            .process_replay(seq_nr, version, &self.event_store);

        sender.do_send(
            Replicated(
                Some(ctx.address().recipient()),
                last_seq_nr,
                events,
            )
        ).expect("Error while sending [REPLICATED] request.");
    }

    pub fn handle_replicated(&mut self) {}
}

impl Actor for Replica {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Replica {} started", self.id);
        self.load_state();
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        println!("Replica {} stopped", self.id);
    }
}

impl Handler<CausalMessage<u64>> for Replica {
    type Result = ();

    fn handle(&mut self, msg: CausalMessage<u64>, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            Command(_, command) => {
                println!("->[COMMAND@{}] with command:{}", self.id, command);
                self.handle_command(command);
            }
            Connect(_, replica_id, other_replica) => {
                println!("->[CONNECT@{}]", self.id);
                self.handle_connect(ctx, replica_id, other_replica);
            }
            Replicate(sender, seq_nr, version) => {
                println!("->[REPLICATE@{}] with seq_nr:{}, version_vector:{}", self.id, seq_nr, version);
                self.handle_replicate(ctx, sender.unwrap(), seq_nr, version);
            }
            Replicated(_, last_seq_nr, events) => {
                println!("->[REPLICATED@{}] with seq_nr:{},n_events:{}", self.id, last_seq_nr, events.len());
                self.handle_replicated();
            }
            _ => {
                println!("Message is not supported.")
            }
        }
    }
}