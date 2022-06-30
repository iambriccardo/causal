use std::cmp;
use std::collections::HashMap;
use std::marker::PhantomData;

use actix::ActorStreamExt;
use itertools::max;

use crate::{Concurrent, Greater, VectorClock};

/** TYPES **/

pub type ReplicaId = usize;
pub type SeqNr = u64;
pub type VTime = VectorClock<ReplicaId>;
pub type ObservedMap = HashMap<ReplicaId, SeqNr>;


/** DATA STRUCTURES **/

pub struct Causal;

pub struct Event<EVENT>
    where EVENT: Clone
{
    pub origin: ReplicaId,
    pub origin_seq_nr: SeqNr,
    pub local_seq_nr: SeqNr,
    pub version: VTime,
    pub data: EVENT,
}

pub struct ReplicaState<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone
{
    pub id: ReplicaId,
    pub seq_nr: SeqNr,
    pub version: VTime,
    pub observed: ObservedMap,
    pub crdt: C,
    _1: PhantomData<STATE>,
    _2: PhantomData<CMD>,
    _3: PhantomData<EVENT>,
}


/** TRAITS **/

pub trait CRDT<STATE, CMD, EVENT>
    where EVENT: Clone
{
    // Creates the default/identity of the CRDT.
    fn default() -> Self;
    // Queries the state of the CRDT.
    fn query(&self) -> STATE;
    // Takes some operation send by the user, and changes it into event.
    fn prepare(&self, command: &CMD) -> EVENT;
    // Called when a new event arrives.
    fn effect(&mut self, event: &Event<EVENT>);
}

pub trait EventStore<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone
{
    fn save_snapshot(&mut self, state: ReplicaState<C, STATE, CMD, EVENT>);
    fn load_snapshot(&self) -> Option<ReplicaState<C, STATE, CMD, EVENT>>;

    fn save_events(&mut self, events: Vec<Event<EVENT>>);
    fn load_events(&self, start_seq_nr: SeqNr) -> Vec<Event<EVENT>>;
}


/** IMPLEMENTATIONS **/

impl<EVENT> Clone for Event<EVENT>
    where EVENT: Clone
{
    fn clone(&self) -> Self {
        Event {
            origin: self.origin,
            origin_seq_nr: self.origin_seq_nr,
            local_seq_nr: self.local_seq_nr,
            version: self.version.clone(),
            data: self.data.clone(),
        }
    }
}

impl Causal {
    // TODO: implement here methods with causal_core's logic.
}

impl<C, STATE, CMD, EVENT> ReplicaState<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone
{
    pub fn new(id: ReplicaId, seq_nr: SeqNr, version: VTime, observed: ObservedMap, crdt: C) -> ReplicaState<C, STATE, CMD, EVENT> {
        ReplicaState {
            id,
            seq_nr,
            version,
            observed,
            crdt,
            _1: PhantomData,
            _2: PhantomData,
            _3: PhantomData,
        }
    }

    pub fn create(id: ReplicaId, crdt: C) -> ReplicaState<C, STATE, CMD, EVENT> {
        ReplicaState::new(
            id,
            0,
            VectorClock::init(),
            HashMap::new(),
            crdt,
        )
    }

    pub fn process_event(&mut self, event: &Event<EVENT>) -> ReplicaState<C, STATE, CMD, EVENT> {
        // We merge the vector clock.
        self.version.merge(self.id, &event.version);
        // We update the point in which we were consuming events from the other machine.
        self.observed.insert(event.origin, event.origin_seq_nr);
        // We dispatch the event to the crdt.
        self.crdt.effect(event);

        return ReplicaState::new(
            self.id,
            cmp::max(self.seq_nr, event.local_seq_nr),
            self.version.clone(),
            self.observed.clone(),
            self.crdt.clone(),
        );
    }

    pub fn process_command(&mut self, command: &CMD, event_store: &mut impl EventStore<C, STATE, CMD, EVENT>) -> ReplicaState<C, STATE, CMD, EVENT> {
        // We increment both the sequence number and the vector clock for this replica.
        let seq_nr = self.seq_nr + 1;
        self.version.increment(self.id);
        // We prepare the data for the event.
        let data = self.crdt.prepare(&command);
        // We create, apply and store the event.
        let event = Event {
            origin: self.id,
            origin_seq_nr: seq_nr,
            local_seq_nr: seq_nr,
            version: self.version.clone(),
            data,
        };
        self.crdt.effect(&event);
        event_store.save_events(vec![event]);

        return ReplicaState::new(
            self.id,
            seq_nr,
            self.version.clone(),
            self.observed.clone(),
            self.crdt.clone(),
        );
    }

    pub fn process_connect(&mut self, replica_id: ReplicaId) -> (SeqNr, VTime) {
        return (
            *self.observed.get(&replica_id).or(Some(&0)).unwrap() + 1,
            self.version.clone()
        );
    }

    // TODO: implement fetch limit in order to have an upper bound.
    pub fn process_replay(&mut self, seq_nr: SeqNr, version: VTime, event_store: &impl EventStore<C, STATE, CMD, EVENT>) -> (SeqNr, Vec<Event<EVENT>>) {
        let mut last_seq_nr = 0;
        let events = event_store.load_events(seq_nr).clone().into_iter().filter(|event| {
            last_seq_nr = cmp::max(last_seq_nr, event.local_seq_nr);
            let comparison = event.version.compare(&version);
            comparison == Greater || comparison == Concurrent
        }).collect();

        return (last_seq_nr, events);
    }

    pub fn unseen(&self, event: &Event<EVENT>) -> bool {
        match self.observed.get(&event.origin) {
            Some(observed_seq_nr) if event.origin_seq_nr > *observed_seq_nr => true,
            _ => {
                let comparison = event.version.compare(&self.version);
                comparison == Greater || comparison == Concurrent
            }
        }
    }
}