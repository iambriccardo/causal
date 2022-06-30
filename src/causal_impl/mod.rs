use std::env::var;
use crate::{CRDT, Event, EventStore};
use crate::causal_actix::ActixCommand;

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

pub struct InMemory;

impl EventStore for InMemory {
    fn save_snapshot<S>(&self, state: S) {

    }

    fn load_snapshot<S>(&self) -> Option<S> {
        None
    }

    fn save_events<E>(&self, events: Vec<Event<E>>) {

    }

    fn load_events<E>(&self, start_seq_nr: u64) -> Vec<Event<E>> {
        vec![]
    }
}