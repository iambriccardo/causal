use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

use crate::{CRDT, Event, EventStore, ReplicaState};
use crate::causal_core::{SeqNr, VTime};
use crate::causal_or_set::SetOperation::{Added, Removed};

pub struct InMemory<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone {
    last_snapshot: Option<ReplicaState<C, STATE, CMD, EVENT>>,
    events: Vec<Event<EVENT>>,
}

impl<C, STATE, CMD, EVENT> InMemory<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone
{
    pub fn create() -> InMemory<C, STATE, CMD, EVENT> {
        InMemory {
            last_snapshot: None,
            events: vec![],
        }
    }
}

pub enum SetCommand<T>
    where T: Clone + Eq + PartialEq + Hash + Display
{
    Add(T),
    Remove(T),
}

#[derive(Clone)]
pub enum SetOperation<T>
    where T: Clone + Eq + PartialEq + Hash + Display
{
    Added(T),
    Removed(HashSet<VTime>),
}

pub struct BinarySet<T>(HashSet<(T, VTime)>) where T: Clone + Eq + PartialEq + Hash + Display;

impl<T> Display for BinarySet<T>
    where T: Clone + Eq + PartialEq + Hash + Display
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut output = String::from("");

        output.push_str("{");
        for value in &self.0 {
            output.push_str(&format!("{},", value.0));
        }
        if !self.0.is_empty() {
            output.pop();
        }
        output.push_str("}");

        write!(f, "{}", output)
    }
}

pub struct ORSet<T>
    where T: Clone + Eq + PartialEq + Hash + Display
{
    elements: BinarySet<T>,
}

impl<T> Clone for ORSet<T>
    where T: Clone + Eq + PartialEq + Hash + Display
{
    fn clone(&self) -> Self {
        ORSet {
            elements: BinarySet(self.elements.0.iter().cloned().collect())
        }
    }
}

impl<T> CRDT<BinarySet<T>, SetCommand<T>, SetOperation<T>> for ORSet<T>
    where T: Clone + Eq + PartialEq + Hash + Display
{
    fn default() -> Self {
        ORSet {
            elements: BinarySet(HashSet::new())
        }
    }

    fn query(&self) -> BinarySet<T> {
        BinarySet(self.elements.0
            .iter()
            .cloned()
            .collect())
    }

    fn prepare(&self, command: &SetCommand<T>) -> SetOperation<T> {
        match command {
            SetCommand::Add(value) => Added(value.clone()),
            SetCommand::Remove(value_to_remove) => {
                Removed(self.elements.0
                    .iter()
                    .filter(|(value, _)| value == value_to_remove)
                    .map(|(_, version)| version)
                    .cloned()
                    .collect()
                )
            }
        }
    }

    fn effect(&mut self, event: &Event<SetOperation<T>>) {
        match &event.data {
            SetOperation::Added(value) => {
                self.elements.0.insert((value.clone(), event.version.clone()));
            }
            SetOperation::Removed(versions) => {
                self.elements = BinarySet(self.elements.0.iter()
                    .filter(|(_, version)| { !versions.contains(&version) })
                    .cloned()
                    .collect());
            }
        }
    }
}

impl<T> EventStore<ORSet<T>, BinarySet<T>, SetCommand<T>, SetOperation<T>> for InMemory<ORSet<T>, BinarySet<T>, SetCommand<T>, SetOperation<T>>
    where T: Clone + Eq + PartialEq + Hash + Display
{
    fn save_snapshot(&mut self, state: &ReplicaState<ORSet<T>, BinarySet<T>, SetCommand<T>, SetOperation<T>>) {
        self.last_snapshot = Some(state.clone())
    }

    fn load_snapshot(&self) -> Option<ReplicaState<ORSet<T>, BinarySet<T>, SetCommand<T>, SetOperation<T>>> {
        match &self.last_snapshot {
            Some(last_snapshot) => Some(last_snapshot.clone()),
            _ => None
        }
    }

    fn save_events(&mut self, events: Vec<Event<SetOperation<T>>>) {
        events.iter().for_each(|event| self.events.push(event.clone()));
    }

    fn load_events(&self, start_seq_nr: SeqNr) -> Vec<Event<SetOperation<T>>> {
        self.events
            .clone()
            .into_iter()
            .filter(|event| {
                event.local_seq_nr >= start_seq_nr
            })
            .collect()
    }
}