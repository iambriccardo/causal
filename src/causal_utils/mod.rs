use crate::{CRDT, Event, ReplicaState};

pub struct InMemory<C, STATE, CMD, EVENT>
    where C: CRDT<STATE, CMD, EVENT> + Clone,
          EVENT: Clone
{
    pub last_snapshot: Option<ReplicaState<C, STATE, CMD, EVENT>>,
    pub events: Vec<Event<EVENT>>,
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