use std::env::var;

use crate::{CRDT, Event, EventStore, ReplicaState};
use crate::causal_actix::ActixCommand;
use crate::causal_core::SeqNr;

pub struct Counter {
    value: u64,
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Counter {
            value: self.value
        }
    }
}

impl CRDT<u64, ActixCommand, u64> for Counter {
    fn default() -> Self {
        Counter {
            value: 0
        }
    }

    fn query(&self) -> u64 {
        self.value
    }

    fn prepare(&self, command: &ActixCommand) -> u64 {
        // For this CRDT, given a specific command we just return 1 which is the increment of the counter.
        match command {
            ActixCommand::Increment => 1,
            _ => 0
        }
    }

    fn effect(&mut self, event: &Event<u64>) {
        self.value = self.value + event.data;
    }
}

pub struct InMemory {
    events: Vec<Event<u64>>,
}

impl InMemory {
    pub fn create() -> InMemory {
        InMemory {
            events: vec![]
        }
    }
}

impl EventStore<Counter, u64, ActixCommand, u64> for InMemory {
    fn save_snapshot(&mut self, state: ReplicaState<Counter, u64, ActixCommand, u64>) {
        todo!()
    }

    fn load_snapshot(&self) -> Option<ReplicaState<Counter, u64, ActixCommand, u64>> {
        None
    }

    fn save_events(&mut self, events: Vec<Event<u64>>) {
        events.iter().for_each(|event| self.events.push(event.clone()));
    }

    fn load_events(&self, start_seq_nr: SeqNr) -> Vec<Event<u64>> {
        self.events
            .clone()
            .into_iter()
            .filter(|event| {
                event.local_seq_nr >= start_seq_nr
            })
            .collect()
    }
}