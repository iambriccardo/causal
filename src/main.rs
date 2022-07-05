use std::{io, thread};
use std::collections::HashMap;

use actix::{Actor, System};

use crate::causal_actix::{Replica, VoidCausalMessage, VoidCausalRecipient};
use crate::causal_actix::ActixCommand::Increment;
use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState};
use crate::causal_impl::{Counter, InMemory};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;
use crate::VoidCausalMessage::{Command, Connect, Query, Sync};

mod causal_time;
mod causal_core;
mod causal_actix;
mod causal_impl;

fn send_void<CMD: Send + Unpin, EVENT: Send + Clone + Unpin>(
    replicas: &HashMap<ReplicaId, VoidCausalRecipient<CMD, EVENT>>,
    replica_id: ReplicaId,
    message: VoidCausalMessage<CMD, EVENT>,
) {
    replicas
        .get(&replica_id)
        .unwrap()
        .do_send(message)
        .expect(&*format!("The delivery of the message to replica {} failed!", replica_id));
}

fn connect<CMD: Send + Unpin, EVENT: Send + Clone + Unpin>(
    replicas: &HashMap<ReplicaId, VoidCausalRecipient<CMD, EVENT>>,
    from: ReplicaId,
    to: ReplicaId,
) {
    send_void(
        replicas,
        from,
        Connect(to, replicas.get(&to).unwrap().clone()),
    );
}

// TODO:
// * Verify whether concurrent replicate requests are good or not for the system (consider the unseen() method).
// * Try a socket based implementation.
// * Implement more complex operation-based CRDTs.
fn main() {
    let replicas_number: usize = 2;
    let system = System::new();
    let mut replicas = HashMap::new();

    let _addr = system.block_on(async {
        // We spawn the replicas.
        for id in 0..replicas_number {
            // For each replica we will craft specific messages that will trigger actions towards the CRDT.
            let replica = Replica::create(
                id,
                Counter::default(),
                InMemory::create(),
            );
            replicas.insert(id, replica.start().recipient());
        }
    });

    // We connect both replicas.
    connect(&replicas, 1, 0);
    connect(&replicas, 0, 1);

    thread::spawn(move || {
        loop {
            println!("Choose an operation ([INC:ID],[QUERY:ID],[SYNC:ID])");

            let mut command = String::new();
            io::stdin()
                .read_line(&mut command)
                .expect("Failed to read from CLI");

            let command_parts: Vec<&str> = command.split(":").collect();
            let action: &str = command_parts.get(0).unwrap();
            let replica_id: &str = command_parts.get(1).unwrap();
            let replica_id: u32 = match replica_id.trim().parse() {
                Ok(value) => value,
                Err(err) => {
                    println!("{}", err);
                    0
                }
            };

            match action {
                "INC" => {
                    send_void(&replicas, replica_id as ReplicaId, Command(Increment));
                }
                "QUERY" => {
                    send_void(&replicas, replica_id as ReplicaId, Query);
                }
                "SYNC" => {
                    send_void(&replicas, replica_id as ReplicaId, Sync);
                }
                &_ => {
                    break;
                }
            };
        }
    });

    system.run().unwrap();
}