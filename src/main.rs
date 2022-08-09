use std::{io, thread};
use std::collections::HashMap;

use actix::{Actor, System};

use crate::causal_actix::{Replica, VoidCausalMessage, VoidCausalRecipient};
use crate::causal_console::{InputField, InputReceiver};
use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState};
use crate::causal_lseq::LSeqCommand;
use crate::causal_or_set::{ORSet, SetCommand};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;
use crate::causal_utils::InMemory;
use crate::VoidCausalMessage::{Command, Connect, Query, Sync};

mod causal_time;
mod causal_core;
mod causal_actix;
mod causal_or_set;
mod causal_console;
mod causal_utils;
mod causal_lseq;

fn send_void<CMD: Send + Unpin, EVENT: Send + Clone + Unpin>(
    replicas: &HashMap<ReplicaId, VoidCausalRecipient<CMD, EVENT>>,
    replica_id: ReplicaId,
    message: VoidCausalMessage<CMD, EVENT>,
) {
    replicas
        .get(&replica_id)
        .unwrap()
        .do_send(message);
        //.expect(&*format!("The delivery of the message to replica {} failed!", replica_id));
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

fn start() {
    let replicas_number: usize = 3;
    let system = System::new();
    let mut replicas = HashMap::new();

    let _addr = system.block_on(async {
        // We spawn the replicas.
        for id in 0..replicas_number {
            // For each replica we will craft specific messages that will trigger actions towards the CRDT.
            let replica = Replica::create(
                id,
                ORSet::<u64>::default(),
                InMemory::create(),
            );
            replicas.insert(id, replica.start().recipient());
        }
    });

    for i in 0..replicas_number {
        for j in (0..replicas_number).rev() {
            if i != j {
                connect(&replicas, i, j);
            }
        }
    }

    // This simple application is just for demonstration purposes. It is not meant to be used.
    thread::spawn(move || {
        loop {
            println!("Choose an operation ([A;VAL:ID],[R;VAL:ID],[I:ID],[Q:ID],[S:ID])");

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
                "Q" => {
                    send_void(&replicas, replica_id as ReplicaId, Query);
                }
                "S" => {
                    send_void(&replicas, replica_id as ReplicaId, Sync);
                }
                &_ => {
                    if action.contains("A") || action.contains("R") {
                        let command_parts: Vec<&str> = action.split(";").collect();
                        let action: &str = command_parts.get(0).unwrap();
                        let value: &str = command_parts.get(1).unwrap();
                        let value: u64 = match value.trim().parse() {
                            Ok(value) => value,
                            Err(err) => {
                                println!("{}", err);
                                0
                            }
                        };

                        match action {
                            "A" => send_void(&replicas, replica_id as ReplicaId, Command(SetCommand::Add(value))),
                            "R" => send_void(&replicas, replica_id as ReplicaId, Command(SetCommand::Remove(value))),
                            &_ => println!("The command is not parsable.")
                        }
                    } else {
                        println!("The command is not parsable.")
                    }
                }
            };
        }
    });

    system.run().unwrap();
}

struct LSeqReceiver {
    commands: Vec<LSeqCommand<char>>
}

impl LSeqReceiver {

    fn new() -> LSeqReceiver {
        LSeqReceiver {
            commands: vec![]
        }
    }
}

impl InputReceiver for LSeqReceiver {
    fn insert_at(&self, position: usize, character: char) {
        println!("Inserting {} at {}", character, position);
    }

    fn remove_at(&self, position: usize) {
        println!("Removing character at {}", position);
    }

    fn on_enter(&self) {
        println!("ENTER")
    }
}

// TODO:
// * Try a socket based implementation.
// * Implement more complex operation-based CRDTs.
fn main() {
    let string = String::from("");
    let receiver = LSeqReceiver::new();
    InputField::start(string, &receiver);
}

