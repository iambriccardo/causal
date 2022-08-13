use std::{io, thread};
use std::collections::HashMap;

use actix::prelude::*;

use crate::causal_actix::{Replica, send_valued, send_void, ValuedCausalMessage, VoidCausalMessage};
use crate::causal_console::{InputField, InputReceiver};
use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState};
use crate::causal_rga::{RGA, RGACommand, RGAOperation, RGAReceiver};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;
use crate::causal_utils::InMemory;
use crate::VoidCausalMessage::{Command, Connect, Sync};

mod causal_time;
mod causal_core;
mod causal_actix;
mod causal_or_set;
mod causal_console;
mod causal_utils;
mod causal_lseq;
mod causal_rga;

fn start() {
    let replicas_number: isize = 3;
    let system = System::new();
    let mut replicas = HashMap::new();

    let _addr = system.block_on(async {
        // We spawn the replicas.
        for id in 0..replicas_number {
            // For each replica we will craft specific messages that will trigger actions towards the CRDT.
            let replica = Replica::create(
                id,
                RGA::<char>::default(Some(id)),
                InMemory::<RGA<char>, Vec<char>, RGACommand<char>, RGAOperation<char>>::create(),
            );
            replicas.insert(id, replica.start());
        }
    });

    // We send a message to replica "from" indicating to connect to replica "to".
    for from in 0..replicas_number {
        for to in (0..replicas_number).rev() {
            if from != to {
                send_void(
                    &replicas,
                    from,
                    Connect(to, replicas
                        .get(&to)
                        .unwrap()
                        .clone()
                        .recipient(),
                    ),
                );
            }
        }
    }

    // This simple application is just for demonstration purposes. It is not meant to be used.
    thread::spawn(move || {
        let _ = System::new();
        let arbiter = Arbiter::new();

        arbiter.spawn(async move {
            loop {
                println!("Choose an operation ([E:ID],[Q:ID],[S:ID])");

                let mut command = String::new();
                io::stdin()
                    .read_line(&mut command)
                    .expect("Failed to read from CLI");

                let command_parts: Vec<&str> = command.split(":").collect();
                let action: &str = command_parts.get(0).unwrap();
                let replica_id: &str = command_parts.get(1).unwrap();
                let replica_id: isize = match replica_id.trim().parse() {
                    Ok(value) => value,
                    Err(err) => {
                        println!("{}", err);
                        0
                    }
                };

                match action {
                    "Q" => {
                        let state = send_valued(
                            &replicas,
                            replica_id,
                            ValuedCausalMessage::Query(Default::default()),
                        ).await;

                        for value in state {
                            print!("{}", value)
                        }
                        println!()
                    }
                    "S" => {
                        send_void(&replicas, replica_id, Sync);
                    }
                    "E" => {
                        let state = send_valued(
                            &replicas,
                            replica_id,
                            ValuedCausalMessage::Query(Default::default()),
                        ).await;

                        let mut receiver = RGAReceiver::new();
                        InputField::start(String::from_iter(state.iter()), &mut receiver);

                        for command in receiver.commands {
                            send_void(&replicas, replica_id, Command(command));
                        }
                    }
                    &_ => println!("The command is not parsable")
                };
            }
        });

        arbiter.join().unwrap();
    });

    system.run().unwrap();
}

// TODO:
// * Try a socket based implementation.
// * Implement more complex operation-based CRDTs.
// * Reduce duplication of in-memory event store.
// * Implement more extensive unit tests for CRDTs.
fn main() {
    start();
}

