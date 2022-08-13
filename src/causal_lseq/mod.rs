use std::cmp;
use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};

use crate::{CRDT, Event, EventStore, InMemory, InputReceiver, ReplicaId, ReplicaState};
use crate::causal_core::SeqNr;
use crate::causal_lseq::LSeqCommand::{Insert, Remove};
use crate::causal_lseq::LSeqOperation::{Inserted, Removed};

type Sequence = Vec<u8>;

pub struct LSeqPtr {
    sequence: Sequence,
    replica_id: ReplicaId,
}

impl LSeqPtr {
    fn from(replica_id: ReplicaId, low: &Sequence, high: &Sequence) -> LSeqPtr {
        LSeqPtr {
            sequence: LSeqPtr::generate_seq(low, high),
            replica_id,
        }
    }

    fn generate_seq(low: &Sequence, high: &Sequence) -> Sequence {
        fn generate(mut mid: Sequence, low: &Sequence, high: &Sequence, i: usize) -> Sequence {
            let min = if i >= low.len() { 0 } else { low[i] };
            let max = if i >= high.len() { 255 } else { high[i] };

            if min + 1 < max {
                mid.insert(i, min + 1);
                mid
            } else {
                mid.insert(i, min);
                generate(mid, low, high, i + 1)
            }
        }

        generate(vec![], low, high, 0)
    }
}

impl Clone for LSeqPtr {
    fn clone(&self) -> Self {
        LSeqPtr {
            sequence: self.sequence.clone(),
            replica_id: self.replica_id.clone(),
        }
    }
}

impl PartialEq<Self> for LSeqPtr {
    fn eq(&self, other: &Self) -> bool {
        let matching = self.sequence.iter().zip(other.sequence.iter()).filter(|&(a, b)| a == b).count();
        matching == self.sequence.len() && matching == other.sequence.len() && self.replica_id == other.replica_id
    }
}

impl PartialOrd for LSeqPtr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let length = cmp::min(self.sequence.len(), other.sequence.len());

        for i in 0..length {
            let self_element = self.sequence[i];
            let other_element = other.sequence[i];

            if self_element < other_element {
                return Some(Less);
            } else if self_element > other_element {
                return Some(Greater);
            }
        }

        return if self.sequence.len() < other.sequence.len() {
            Some(Less)
        } else if self.sequence.len() > other.sequence.len() {
            Some(Greater)
        } else {
            if self.replica_id < other.replica_id {
                Some(Less)
            } else {
                Some(Greater)
            }
        };
    }
}

pub enum LSeqCommand<T>
    where T: Clone
{
    Insert(usize, ReplicaId, T),
    Remove(usize),
}

#[derive(Clone)]
pub enum LSeqOperation<T>
    where T: Clone
{
    Inserted(LSeqPtr, T),
    Removed(LSeqPtr),
}

pub struct LSeq<T>
    where T: Clone
{
    elements: Vec<(LSeqPtr, T)>,
}

impl<T> Clone for LSeq<T>
    where T: Clone
{
    fn clone(&self) -> Self {
        LSeq {
            elements: self.elements.iter().cloned().collect()
        }
    }
}

impl<T> CRDT<Vec<T>, LSeqCommand<T>, LSeqOperation<T>> for LSeq<T>
    where T: Clone
{
    fn default(_: Option<ReplicaId>) -> Self {
        LSeq {
            elements: vec![]
        }
    }

    fn query(&self) -> Vec<T> {
        self.elements
            .iter()
            .cloned()
            .map(|(_, value)| value)
            .collect()
    }

    fn prepare(&self, command: &LSeqCommand<T>) -> LSeqOperation<T> {
        match command {
            Insert(index, replica_id, value) => {
                let empty_v_ptr = vec![];

                let left = if *index == 0 { &empty_v_ptr } else { &self.elements[*index - 1].0.sequence };
                let right = if *index >= self.elements.len() { &empty_v_ptr } else { &self.elements[*index].0.sequence };

                println!("Inserting at pos {}", index);

                Inserted(LSeqPtr::from(replica_id.clone(), &left, &right), value.clone())
            }
            Remove(index) => {
                Removed(self.elements[*index].0.clone())
            }
        }
    }

    fn effect(&mut self, event: &Event<LSeqOperation<T>>) {
        match &event.data {
            Inserted(ins_v_ptr, value) => {
                let index = self.elements
                    .iter()
                    .position(|(v_ptr, _)| ins_v_ptr <= v_ptr)
                    .or(Some(self.elements.len()))
                    .unwrap();

                self.elements.insert(index, (ins_v_ptr.clone(), value.clone()));
            }
            Removed(rem_v_ptr) => {
                let index = self.elements
                    .iter()
                    .position(|(v_ptr, _)| rem_v_ptr == v_ptr)
                    .expect("Couldn't find position of the character to delete.");

                self.elements.remove(index);
            }
        }
    }
}

impl<T> EventStore<LSeq<T>, Vec<T>, LSeqCommand<T>, LSeqOperation<T>> for InMemory<LSeq<T>, Vec<T>, LSeqCommand<T>, LSeqOperation<T>>
    where T: Clone
{
    fn save_snapshot(&mut self, state: &ReplicaState<LSeq<T>, Vec<T>, LSeqCommand<T>, LSeqOperation<T>>) {
        self.last_snapshot = Some(state.clone())
    }

    fn load_snapshot(&self) -> Option<ReplicaState<LSeq<T>, Vec<T>, LSeqCommand<T>, LSeqOperation<T>>> {
        match &self.last_snapshot {
            Some(last_snapshot) => Some(last_snapshot.clone()),
            _ => None
        }
    }

    fn save_events(&mut self, events: Vec<Event<LSeqOperation<T>>>) {
        events.iter().for_each(|event| self.events.push(event.clone()));
    }

    fn load_events(&self, start_seq_nr: SeqNr) -> Vec<Event<LSeqOperation<T>>> {
        self.events
            .clone()
            .into_iter()
            .filter(|event| {
                event.local_seq_nr >= start_seq_nr
            })
            .collect()
    }
}

pub struct LSeqReceiver {
    pub replica_id: ReplicaId,
    pub commands: Vec<LSeqCommand<char>>,
}

impl LSeqReceiver {
    #[allow(dead_code)]
    pub fn new(replica_id: ReplicaId) -> LSeqReceiver {
        LSeqReceiver {
            replica_id,
            commands: vec![],
        }
    }
}

impl InputReceiver for LSeqReceiver {
    fn insert_at(&mut self, position: usize, character: char) {
        self.commands.push(Insert(position, self.replica_id, character));
    }

    fn remove_at(&mut self, position: usize) {
        self.commands.push(Remove(position))
    }
}

#[cfg(test)]
mod tests {
    use crate::causal_lseq::{LSeqPtr, Sequence};

    #[test]
    fn test_generate_seq_empty() {
        let low = LSeqPtr::new(0);
        let high = LSeqPtr::new(1);

        let expected_sequence: Sequence = vec![1];
        let sequence = LSeqPtr::generate_seq(&low.sequence, &high.sequence);

        assert_eq!(expected_sequence, sequence);
    }

    #[test]
    fn test_generate_seq_start() {
        let low = LSeqPtr::new(0);
        let mut high = LSeqPtr::new(1);
        high.sequence = vec![1];

        let expected_sequence: Sequence = vec![0, 1];
        let sequence = LSeqPtr::generate_seq(&low.sequence, &high.sequence);

        assert_eq!(expected_sequence, sequence);
    }

    #[test]
    fn test_generate_seq_end() {
        let mut low = LSeqPtr::new(0);
        low.sequence = vec![1];
        let high = LSeqPtr::new(1);

        let expected_sequence: Sequence = vec![2];
        let sequence = LSeqPtr::generate_seq(&low.sequence, &high.sequence);

        assert_eq!(expected_sequence, sequence);
    }

    #[test]
    fn test_generate_seq_middle() {
        let mut low = LSeqPtr::new(0);
        low.sequence = vec![1];
        let mut high = LSeqPtr::new(1);
        high.sequence = vec![2];

        let expected_sequence: Sequence = vec![1, 1];
        let sequence = LSeqPtr::generate_seq(&low.sequence, &high.sequence);

        assert_eq!(expected_sequence, sequence);
    }

    #[test]
    fn test_eq() {
        let mut low = LSeqPtr::new(0);
        low.sequence = vec![1];
        let mut high = LSeqPtr::new(0);
        high.sequence = vec![1];

        assert!(low == high);
    }

    #[test]
    fn test_ord() {
        let mut low = LSeqPtr::new(0);
        low.sequence = vec![1];
        let mut high = LSeqPtr::new(0);
        high.sequence = vec![2];

        assert!(low < high);
    }

    #[test]
    fn test_multiple_ord() {
        let mut low = LSeqPtr::new(0);
        low.sequence = vec![1, 1];
        let mut high = LSeqPtr::new(0);
        high.sequence = vec![1, 2];

        assert!(low < high);
    }

    #[test]
    fn test_ord_different_lengths() {
        let mut low = LSeqPtr::new(0);
        low.sequence = vec![1, 1];
        let mut high = LSeqPtr::new(0);
        high.sequence = vec![1, 1, 1];

        assert!(low < high);
    }
}