use std::cmp;
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::{Concurrent, Greater, VectorClock};

/** TYPES **/
pub type ReplicaId = usize;
pub type SeqNr = u64;
pub type VTime = VectorClock<ReplicaId>;
pub type ObservedMap = HashMap<ReplicaId, SeqNr>;


/** DATA STRUCTURES **/
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
    fn save_snapshot(&mut self, state: &ReplicaState<C, STATE, CMD, EVENT>);
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

impl<C, STATE, CMD, EVENT> Clone for ReplicaState<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone {
    fn clone(&self) -> Self {
        ReplicaState {
            id: self.id,
            seq_nr: self.seq_nr,
            version: self.version.clone(),
            observed: self.observed.clone(),
            crdt: self.crdt.clone(),
            _1: PhantomData,
            _2: PhantomData,
            _3: PhantomData,
        }
    }
}

impl<C, STATE, CMD, EVENT> ReplicaState<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone
{
    pub fn new(
        id: ReplicaId,
        seq_nr: SeqNr,
        version: VTime,
        observed: ObservedMap,
        crdt: C,
    ) -> ReplicaState<C, STATE, CMD, EVENT> {
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

    pub fn process_command(
        &mut self,
        command: &CMD,
        event_store: &mut impl EventStore<C, STATE, CMD, EVENT>,
    ) -> ReplicaState<C, STATE, CMD, EVENT> {
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

    // pub fn process_connect(&mut self, replica_id: ReplicaId) -> (SeqNr, VTime) {
    //     return (
    //         // We send the next seq number we want. If we didn't observe anything, it will be 1.
    //         *self.observed.get(&replica_id).or(Some(&0)).unwrap() + 1,
    //         // We send our version vector because it will be used to avoid duplicates being sent.
    //         self.version.clone()
    //     );
    // }

    pub fn process_sync(&mut self, replica_id: ReplicaId) -> (ReplicaId, SeqNr, VTime) {
        return (
            self.id,
            // We send the next seq number we want. If we didn't observe anything, it will be 1.
            *self.observed.get(&replica_id).or(Some(&0)).unwrap() + 1,
            // We send our version vector because it will be used to avoid duplicates being sent.
            self.version.clone()
        );
    }

    // TODO: implement fetch limit in order to have an upper bound.
    pub fn process_replay(
        &mut self,
        seq_nr: SeqNr,
        version: VTime,
        event_store: &impl EventStore<C, STATE, CMD, EVENT>,
    ) -> (ReplicaId, SeqNr, Vec<Event<EVENT>>) {
        // By default the last seq nr is 0 and it will change in case some events are consumed.
        let mut last_seq_nr = 0;
        let events = event_store
            .load_events(seq_nr)
            .clone()
            .into_iter()
            .filter(|event| {
                // For each event we keep track of the maximum seq nr seen so far.
                last_seq_nr = cmp::max(last_seq_nr, event.local_seq_nr);
                let comparison = event.version.compare(&version);
                comparison == Greater || comparison == Concurrent
            })
            .collect();

        return (self.id, last_seq_nr, events);
    }

    pub fn process_replicated(
        &mut self,
        sender: ReplicaId,
        _: SeqNr, // The last_seq_nr is not used for now because we just compute it.
        events: Vec<Event<EVENT>>,
        event_store: &mut impl EventStore<C, STATE, CMD, EVENT>,
    ) -> Option<ReplicaState<C, STATE, CMD, EVENT>> {
        // We get the last observed seq nr of the sender, that is, the replica sending us new events.
        let mut remote_seq_nr = self.observed
            .get(&sender)
            .or(Some(&0))
            .unwrap()
            .clone();
        let mut new_state = None;

        // We filter all the events received by unseen because in case of concurrency we might have
        // received some duplicates.
        let unseen_events = events
            .into_iter()
            .filter(|event| self.unseen(event))
            .collect::<Vec<Event<EVENT>>>();

        let new_events = unseen_events
            .into_iter()
            .map(|event| {
                // We increment the local seq nr.
                let seq_nr = self.seq_nr + 1;
                // We merge the version vector with the incoming vector.
                self.version.merge(self.id, &event.version);
                // We compute the seq nr until which we have read all the events from sender.
                remote_seq_nr = cmp::max(remote_seq_nr, event.local_seq_nr);
                // We save the last remote seq nr we know to have read.
                self.observed.insert(sender, remote_seq_nr);
                // We perform the effect on the crdt.
                self.crdt.effect(&event);
                // We compute the new replica state.
                new_state = Some(ReplicaState::new(
                    self.id,
                    seq_nr,
                    self.version.clone(),
                    self.observed.clone(),
                    self.crdt.clone(),
                ));
                // We create a new event to be applied locally with the newly updated local seq nr.
                Event {
                    origin: event.origin,
                    origin_seq_nr: event.origin_seq_nr,
                    local_seq_nr: seq_nr,
                    version: event.version.clone(),
                    data: event.data.clone(),
                }
            })
            .collect::<Vec<Event<EVENT>>>();

        // We store all the modified events into the event store.
        event_store.save_events(new_events);

        return new_state;
    }

    pub fn unseen(&self, event: &Event<EVENT>) -> bool {
        // TODO: suspicious implementation which doesn't work well in concurrent cases.
        //
        // Supposing we have three replicas 1,2,3 with this initial state (replica 2 pulled from 1):
        // replica_id -> local_seq_nr-event [vector_clock] [observed]
        // 1 -> 1-A [1,0,0] []
        // 2 -> 1-A [1,0,0] [1:1]
        // 3 -> empty
        // Suppose now 3 sends a [REPLICATE] request concurrently to both 1 and 2 (containing the vector clock [0,0,0]) and the answer from 2 is applied first:
        // 1 -> 1-A [1,0,0] []
        // 2 -> 1-A [1,0,0] [1:1]
        // 3 -> 1-A [1,0,0] [2:1]
        // Then answer from 1 is received and unseen() call sees that we observed 0 from 1 and we received an event with seq nr 1, therefore
        // we apply this event resulting in:
        // 1 -> 1-A [1,0,0] []
        // 2 -> 1-A [1,0,0] [1:1]
        // 3 -> 1-A [1,0,0] 2-A [1,0,0] [2:1,1:1] **DUPLICATE EVENT**
        //
        // A possible solution would be to check only the vector clock of the element but this would need further testing.
        match self.observed.get(&event.origin) {
            Some(observed_seq_nr) if event.origin_seq_nr > *observed_seq_nr => true,
            _ => {
                let comparison = event.version.compare(&self.version);
                comparison == Greater || comparison == Concurrent
            }
        }
    }

    pub fn process_query(&self) -> STATE {
        self.crdt.query()
    }
}