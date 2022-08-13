use std::cmp;
use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};

use crate::{CRDT, Event, EventStore, InMemory, InputReceiver, ReplicaId, ReplicaState};
use crate::causal_core::SeqNr;
use crate::causal_rga::RGACommand::{Insert, Remove};
use crate::causal_rga::RGAOperation::{Inserted, Removed};

pub struct RGAPtr {
    seq_nr: SeqNr,
    replica_id: ReplicaId,
}

impl RGAPtr {
    fn new(replica_id: ReplicaId) -> RGAPtr {
        RGAPtr {
            seq_nr: 0,
            replica_id,
        }
    }

    fn next_seq_nr(&self) -> Self {
        let mut clone = self.clone();
        clone.seq_nr += 1;
        clone
    }

    fn update_highest_observed_seq_nr(&mut self, other_v_ptr: &RGAPtr) {
        self.seq_nr = cmp::max(self.seq_nr, other_v_ptr.seq_nr);
    }
}

impl Clone for RGAPtr {
    fn clone(&self) -> Self {
        RGAPtr {
            seq_nr: self.seq_nr.clone(),
            replica_id: self.replica_id.clone(),
        }
    }
}

impl PartialEq<Self> for RGAPtr {
    fn eq(&self, other: &Self) -> bool {
        self.seq_nr == other.seq_nr && self.replica_id == other.replica_id
    }
}

impl PartialOrd<Self> for RGAPtr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.seq_nr < other.seq_nr {
            Some(Less)
        } else if self.seq_nr > other.seq_nr {
            Some(Greater)
        } else {
            if self.replica_id < other.replica_id {
                Some(Less)
            } else if self.replica_id > other.replica_id {
                Some(Greater)
            } else {
                None
            }
        }
    }
}

pub enum RGACommand<T>
    where T: Clone
{
    Insert(usize, T),
    Remove(usize),
}

#[derive(Clone)]
pub enum RGAOperation<T>
    where T: Clone
{
    Inserted(RGAPtr, RGAPtr, T),
    Removed(RGAPtr),
}

pub struct RGA<T>
    where T: Clone
{
    sequencer: RGAPtr,
    elements: Vec<(RGAPtr, Option<T>)>,
}

impl<T> RGA<T>
    where T: Clone
{
    fn index_with_tombstones(&self, index: usize) -> usize {
        let mut offset = 1;
        let mut remaining = index;

        while offset < self.elements.len() && remaining > 0 {
            let element = &self.elements[offset];
            if let Some(_) = element.1 {
                remaining -= 1;
            }
            offset += 1;
        }

        offset
    }

    fn index_of_v_ptr(&self, v_ptr: &RGAPtr) -> usize {
        self.elements
            .iter()
            .position(|(inner_v_ptr, _)| v_ptr == inner_v_ptr)
            .expect("Couldn't find position of with the given virtual pointer.")
    }

    fn shift(&self, offset: usize, v_ptr: &RGAPtr) -> usize {
        // If we append at the end, we don't need any shift.
        let mut offset = offset;
        while offset < self.elements.len() && *v_ptr < self.elements[offset].0 {
            offset += 1;
        }
        offset
    }
}

impl<T> Clone for RGA<T>
    where T: Clone
{
    fn clone(&self) -> Self {
        RGA {
            sequencer: self.sequencer.clone(),
            elements: self.elements.iter().cloned().collect(),
        }
    }
}

impl<T> CRDT<Vec<T>, RGACommand<T>, RGAOperation<T>> for RGA<T>
    where T: Clone
{
    fn default(replica_id: Option<ReplicaId>) -> Self {
        let replica_id = replica_id.expect("You must set a valid replica id for RGA to work.");

        RGA {
            sequencer: RGAPtr::new(replica_id),
            // The base pointer is the default (0,-1) which must be the same across all the replicas.
            elements: vec![(RGAPtr::new(-1), None)],
        }
    }

    fn query(&self) -> Vec<T> {
        self.elements
            .iter()
            .cloned()
            .filter(|(_, value)| value.is_some())
            .map(|(_, value)| value.unwrap())
            .collect()
    }

    fn prepare(&self, command: &RGACommand<T>) -> RGAOperation<T> {
        match command {
            Insert(index, value) => {
                let index = self.index_with_tombstones(*index);
                let prev_v_ptr = self.elements[index - 1].0.clone();
                let at_v_ptr = self.sequencer.next_seq_nr();

                Inserted(prev_v_ptr, at_v_ptr, value.clone())
            }
            Remove(index) => {
                let index = self.index_with_tombstones(*index);
                let at_vt_ptr = self.elements[index].0.clone();

                Removed(at_vt_ptr)
            }
        }
    }

    fn effect(&mut self, event: &Event<RGAOperation<T>>) {
        match &event.data {
            Inserted(prev_v_ptr, at_v_ptr, value) => {
                let predecessor_index = self.index_of_v_ptr(prev_v_ptr);
                let insert_index = self.shift(predecessor_index + 1, at_v_ptr);
                self.sequencer.update_highest_observed_seq_nr(at_v_ptr);
                self.elements.insert(insert_index, (at_v_ptr.clone(), Some(value.clone())));
            }
            Removed(at_v_ptr) => {
                let index = self.index_of_v_ptr(at_v_ptr);
                let mut element = self.elements[index].clone();
                element.1 = None;
                self.elements[index] = element;
            }
        }
    }
}

impl<T> EventStore<RGA<T>, Vec<T>, RGACommand<T>, RGAOperation<T>> for InMemory<RGA<T>, Vec<T>, RGACommand<T>, RGAOperation<T>>
    where T: Clone
{
    fn save_snapshot(&mut self, state: &ReplicaState<RGA<T>, Vec<T>, RGACommand<T>, RGAOperation<T>>) {
        self.last_snapshot = Some(state.clone())
    }

    fn load_snapshot(&self) -> Option<ReplicaState<RGA<T>, Vec<T>, RGACommand<T>, RGAOperation<T>>> {
        match &self.last_snapshot {
            Some(last_snapshot) => Some(last_snapshot.clone()),
            _ => None
        }
    }

    fn save_events(&mut self, events: Vec<Event<RGAOperation<T>>>) {
        events.iter().for_each(|event| self.events.push(event.clone()));
    }

    fn load_events(&self, start_seq_nr: SeqNr) -> Vec<Event<RGAOperation<T>>> {
        self.events
            .clone()
            .into_iter()
            .filter(|event| {
                event.local_seq_nr >= start_seq_nr
            })
            .collect()
    }
}

pub struct RGAReceiver {
    pub commands: Vec<RGACommand<char>>,
}

impl RGAReceiver {
    pub fn new() -> RGAReceiver {
        RGAReceiver {
            commands: vec![],
        }
    }
}

impl InputReceiver for RGAReceiver {
    fn insert_at(&mut self, position: usize, character: char) {
        self.commands.push(Insert(position, character));
    }

    fn remove_at(&mut self, position: usize) {
        self.commands.push(Remove(position))
    }
}