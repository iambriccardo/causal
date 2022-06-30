use std::collections::HashMap;
use std::fmt::format;
use std::ops::Add;
use std::path::Component::CurDir;

use actix::{Actor, Addr, AsyncContext, Context, Handler, Recipient, System};
use actix::prelude::*;

use crate::causal_actix::{CausalMessage, CausalReceiver, Replica};
use crate::causal_actix::ActixCommand::Increment;
use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;
use crate::CausalMessage::Connect;

mod causal_time;
mod causal_core;
mod causal_actix;
mod causal_impl;

fn send(replicas: &HashMap<ReplicaId, CausalReceiver>, replica_id: ReplicaId, message: CausalMessage) {
    replicas
        .get(&replica_id)
        .unwrap()
        .do_send(message)
        .expect(&*format!("The delivery of the message to replica {} failed!", replica_id));
}

fn main() {
    let replicas_number: usize = 2;
    let system = System::new();
    let mut replicas = HashMap::new();

    let _addr = system.block_on(async {
        // We spawn the replicas.
        for id in 0..replicas_number {
            // For each replica we will craft specific messages that will trigger actions towards the CRDT.
            replicas.insert(id, Replica::start_and_receive(id));
        }
    });

    send(
        &replicas,
        0,
        Connect(None, 1, replicas.get(&1).unwrap().clone())
    );

    send(
        &replicas,
        1,
        Connect(None, 0, replicas.get(&0).unwrap().clone())
    );

    system.run().unwrap();
}