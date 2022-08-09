use std::collections::HashMap;
use std::fmt::Display;

use actix::{Actor, Context, Handler, Recipient};
use actix::prelude::*;

use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState, SeqNr, VTime};
use crate::VoidCausalMessage::{Command, Connect, Replicate, Replicated, Sync};

/** TYPES **/
pub type VoidReplicasTable<CMD, EVENT> = HashMap<ReplicaId, Recipient<VoidCausalMessage<CMD, EVENT>>>;
pub type VoidCausalRecipient<CMD, EVENT> = Recipient<VoidCausalMessage<CMD, EVENT>>;


/** MESSAGES **/
#[derive(Message)]
#[rtype(result = "()")]
pub enum VoidCausalMessage<CMD, EVENT>
    where CMD: Send + Unpin,
          EVENT: Send + Clone
{
    // Message that represents the execution of a command in the receiving replica.
    Command(CMD),
    // Message that represents the connection of the receiving replica to another.
    Connect(ReplicaId, VoidCausalRecipient<CMD, EVENT>),
    // Message that represents the start of sync between the receiving replica and all the replicas connected.
    Sync,
    // Message that represents a request to replicate the content of the receiving replica.
    Replicate(ReplicaId, SeqNr, VTime),
    // Message that represents the replicated events that the receiving replica will apply locally.
    Replicated(ReplicaId, SeqNr, Vec<Event<EVENT>>),
    // Message that represents the querying of the replica's crdt state.
    Query,
}

/** ACTORS **/
pub struct Replica<C: 'static, STATE: 'static, CMD: 'static, EVENT: 'static, STORE: 'static>
    where C: CRDT<STATE, CMD, EVENT> + Clone + Unpin,
          STATE: Display + Unpin,
          CMD: Send + Unpin,
          EVENT: Send + Clone + Unpin,
          STORE: EventStore<C, STATE, CMD, EVENT> + Unpin
{
    // Init params are used for injecting parameters before the state loading.
    init_id: ReplicaId,
    init_crdt: C,
    replica_state: Option<ReplicaState<C, STATE, CMD, EVENT>>,
    replicating_nodes: VoidReplicasTable<CMD, EVENT>,
    event_store: STORE,
}


/** IMPLEMENTATIONS **/
impl<C: 'static, STATE: 'static, CMD: 'static, EVENT: 'static, STORE: 'static> Replica<C, STATE, CMD, EVENT, STORE>
    where C: CRDT<STATE, CMD, EVENT> + Clone + Unpin,
          STATE: Display + Unpin,
          CMD: Send + Unpin,
          EVENT: Send + Clone + Unpin,
          STORE: EventStore<C, STATE, CMD, EVENT> + Unpin
{
    pub fn create(id: ReplicaId, crdt: C, store: STORE) -> Replica<C, STATE, CMD, EVENT, STORE> {
        Replica {
            init_id: id,
            init_crdt: crdt,
            replica_state: None,
            replicating_nodes: HashMap::new(),
            event_store: store,
        }
    }

    pub fn load_state(&mut self) {
        // TODO: implement a more efficient state initialization.
        let mut state = self.event_store
            .load_snapshot()
            .or(Some(ReplicaState::create(self.init_id, self.init_crdt.clone())))
            .unwrap();

        for event in self.event_store.load_events(state.seq_nr + 1) {
            state = state.process_event(&event);
        }

        self.replica_state = Some(state);
    }

    pub fn handle_command(&mut self, command: CMD) {
        let state = self.replica_state
            .as_mut()
            .unwrap()
            .process_command(&command, &mut self.event_store);

        self.replica_state = Some(state);
    }

    pub fn handle_connect(
        &mut self,
        replica_id: ReplicaId,
        replica_receiver: VoidCausalRecipient<CMD, EVENT>,
    ) {
        self.replicating_nodes.insert(replica_id, replica_receiver.clone());
    }

    pub fn handle_sync(&mut self) {
        for (replica_id, replica_receiver) in &self.replicating_nodes {
            let (current_replica_id, seq_nr, version) = self.replica_state
                .as_mut()
                .unwrap()
                .process_sync(replica_id.clone());

            replica_receiver
                .do_send(Replicate(current_replica_id, seq_nr, version));
                // .expect(&*format!("Error while sending [REPLICATE] request from {} to {}", self.init_id, replica_id));
        }
    }

    pub fn handle_replicate(
        &mut self,
        sender: ReplicaId,
        seq_nr: SeqNr,
        version: VTime,
    ) {
        let (current_replica_id, last_seq_nr, events) = self.replica_state
            .as_mut()
            .unwrap()
            .process_replay(seq_nr, version, &self.event_store);

        self.replicating_nodes
            .get(&sender)
            .expect(&*format!("Replica {} doesn't know about replica {}", self.init_id, sender))
            .do_send(Replicated(current_replica_id, last_seq_nr, events));
            // .expect("Error while sending [REPLICATED] request.");
    }

    pub fn handle_replicated(
        &mut self,
        sender: ReplicaId,
        last_seq_nr: SeqNr,
        events: Vec<Event<EVENT>>,
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

    pub fn handle_query(&mut self) {
        let state = self.replica_state
            .as_mut()
            .unwrap()
            .process_query();

        println!("STATE@{} = {}", self.init_id, state)
    }
}

impl<C: 'static, STATE: 'static, CMD: 'static, EVENT: 'static, STORE: 'static> Actor for Replica<C, STATE, CMD, EVENT, STORE>
    where C: CRDT<STATE, CMD, EVENT> + Clone + Unpin,
          STATE: Display + Unpin,
          CMD: Send + Unpin,
          EVENT: Send + Clone + Unpin,
          STORE: EventStore<C, STATE, CMD, EVENT> + Unpin
{
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        self.load_state();
    }
}

impl<C: 'static, STATE: 'static, CMD: 'static, EVENT: 'static, STORE: 'static> Handler<VoidCausalMessage<CMD, EVENT>> for Replica<C, STATE, CMD, EVENT, STORE>
    where C: CRDT<STATE, CMD, EVENT> + Clone + Unpin,
          STATE: Display + Unpin,
          CMD: Send + Unpin,
          EVENT: Send + Clone + Unpin,
          STORE: EventStore<C, STATE, CMD, EVENT> + Unpin
{
    type Result = ();

    fn handle(&mut self, msg: VoidCausalMessage<CMD, EVENT>, _: &mut Self::Context) -> Self::Result {
        match msg {
            Command(command) => {
                println!("APP-[COMMAND]->@{}", self.init_id);
                self.handle_command(command);
            }
            Connect(replica_id, replica_receiver) => {
                println!("APP-[CONNECT]->@{} with replica_id: {}", self.init_id, replica_id);
                self.handle_connect(replica_id, replica_receiver);
            }
            Sync => {
                println!("APP-[SYNC]->@{}", self.init_id);
                self.handle_sync();
            }
            Replicate(sender, seq_nr, version) => {
                println!("@{}-[REPLICATE]->@{} with seq_nr:{}, version_vector:{}", sender, self.init_id, seq_nr, version);
                self.handle_replicate(sender, seq_nr, version);
            }
            Replicated(sender, last_seq_nr, events) => {
                println!("@{}-[REPLICATED]->@{} with last_seq_nr:{}, n_events:{}", sender, self.init_id, last_seq_nr, events.len());
                self.handle_replicated(sender, last_seq_nr, events);
            }
            VoidCausalMessage::Query => {
                println!("APP-[QUERY]->@{}", self.init_id);
                self.handle_query();
            }
        }
    }
}