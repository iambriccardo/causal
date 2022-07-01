use std::collections::HashMap;

use actix::System;

use crate::causal_actix::{CausalMessage, CausalReceiver, Replica};
use crate::causal_actix::ActixCommand::Increment;
use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;
use crate::CausalMessage::{Command, Connect};

mod causal_time;
mod causal_core;
mod causal_actix;
mod causal_impl;

fn send<EVENT: Send + Clone>(replicas: &HashMap<ReplicaId, CausalReceiver<EVENT>>, replica_id: ReplicaId, message: CausalMessage<EVENT>) {
    replicas
        .get(&replica_id)
        .unwrap()
        .do_send(message)
        .expect(&*format!("The delivery of the message to replica {} failed!", replica_id));
}

fn connect<EVENT: Send + Clone>(replicas: &HashMap<ReplicaId, CausalReceiver<EVENT>>, from: ReplicaId, to: ReplicaId) {
    send(
        replicas,
        from,
        Connect(to, replicas.get(&to).unwrap().clone()),
    );
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

    // We make two increments in replica 0.
    send(&replicas, 0, Command(Increment));
    send(&replicas, 0, Command(Increment));

    // We make two increments in replica 1.
    send(&replicas, 1, Command(Increment));

    // We connect replica 1 -> 0 in order to fetch data.
    connect(&replicas, 1, 0);
    connect(&replicas, 0, 1);

    system.run().unwrap();
}