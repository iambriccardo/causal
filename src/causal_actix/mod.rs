use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use actix::{Actor, Context, Handler, Recipient};
use actix::prelude::*;

use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState, SeqNr, VTime};
use crate::causal_impl::{Counter, InMemory};
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
            _ => output.push_str("increment")
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
    Command(ActixCommand),
    // Message that represents the connection of the receiving replica to another.
    Connect(ReplicaId, CausalReceiver<EVENT>),
    // Message that represents a request to replicate the content of the receiving replica.
    Replicate(Option<ReplicaId>, SeqNr, VTime),
    // Message that represents the replicated events that the receiving replica will apply locally.
    Replicated(Option<ReplicaId>, SeqNr, Vec<Event<EVENT>>),
}


/** ACTORS **/

type ReplicaEventType = u64;

pub struct Replica {
    id: ReplicaId,
    // TODO: replace with generic CRDT.
    replica_state: Option<ReplicaState<Counter, u64, ActixCommand, ReplicaEventType>>,
    replicating_nodes: ReplicasTable<ReplicaEventType>,
    event_store: InMemory,
}

impl Replica {
    pub fn start_and_receive(id: ReplicaId) -> CausalReceiver<ReplicaEventType> {
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

    pub fn handle_connect(
        &mut self,
        replica_id: ReplicaId,
        replica_receiver: CausalReceiver<ReplicaEventType>,
    ) {
        self.replicating_nodes.insert(replica_id, replica_receiver.clone());

        let (seq_nr, version) = self.replica_state
            .as_mut()
            .unwrap()
            .process_connect(replica_id);

        replica_receiver.do_send(
            Replicate(
                Some(self.replica_state.as_ref().unwrap().id),
                seq_nr,
                version,
            )
        ).expect("Error while sending [REPLICATE] request.")
    }

    pub fn handle_replicate(
        &mut self,
        sender: ReplicaId,
        seq_nr: SeqNr,
        version: VTime,
    ) {
        let (last_seq_nr, events) = self.replica_state
            .as_mut()
            .unwrap()
            .process_replay(seq_nr, version, &self.event_store);

        self.replicating_nodes
            .get(&sender)
            .expect(&*format!("Replica {} doesn't know about replica {}", self.id, sender))
            .do_send(Replicated(
                Some(self.replica_state.as_ref().unwrap().id),
                last_seq_nr,
                events,
            )
            ).expect("Error while sending [REPLICATED] request.");
    }

    pub fn handle_replicated(
        &mut self,
        sender: ReplicaId,
        last_seq_nr: SeqNr,
        events: Vec<Event<ReplicaEventType>>,
    ) {
        let state = self.replica_state
            .as_mut()
            .unwrap()
            .process_replicated(sender, last_seq_nr, events, &mut self.event_store);

        // If a new state has been created as a result of the events received, we are going to apply
        // it to the replica.
        if let Some(new_state) = state {
            self.replica_state = Some(new_state);
        }
    }
}

impl Actor for Replica {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        self.load_state();
    }
}

impl Handler<CausalMessage<u64>> for Replica {
    type Result = ();

    fn handle(&mut self, msg: CausalMessage<u64>, _: &mut Self::Context) -> Self::Result {
        match msg {
            Command(command) => {
                println!("APP-[COMMAND]->@{} with command:{}", self.id, command);
                self.handle_command(command);
            }
            Connect(replica_id, replica_receiver) => {
                println!("APP-[CONNECT]->@{} with replica_id: {}", self.id, replica_id);
                self.handle_connect(replica_id, replica_receiver);
            }
            Replicate(sender, seq_nr, version) => {
                println!("@{}-[REPLICATE]->@{} with seq_nr:{}, version_vector:{}", sender.unwrap(), self.id, seq_nr, version);
                self.handle_replicate(sender.unwrap(), seq_nr, version);
            }
            Replicated(sender, last_seq_nr, events) => {
                println!("@{}-[REPLICATED]->@{} with seq_nr:{}, n_events:{}", sender.unwrap(), self.id, last_seq_nr, events.len());
                self.handle_replicated(sender.unwrap(), last_seq_nr, events);
            }
        }
    }
}