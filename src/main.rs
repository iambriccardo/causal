use std::{io, thread};
use std::collections::HashMap;

use actix::prelude::*;

use crate::causal_actix::{Replica, ValuedCausalMessage, VoidCausalMessage};
use crate::causal_console::{InputField, InputReceiver};
use crate::causal_core::{CRDT, Event, EventStore, ReplicaId, ReplicaState};
use crate::causal_lseq::{LSeq, LSeqCommand, LSeqOperation};
use crate::causal_time::ClockComparison::{Concurrent, Greater};
use crate::causal_time::VectorClock;
use crate::causal_utils::InMemory;
use crate::LSeqCommand::{Insert, Remove};
use crate::VoidCausalMessage::{Command, Connect, Sync};

mod causal_time;
mod causal_core;
mod causal_actix;
mod causal_or_set;
mod causal_console;
mod causal_utils;
mod causal_lseq;

fn send_void<C, STATE, CMD, EVENT, STORE>(
    replicas: &HashMap<ReplicaId, Addr<Replica<C, STATE, CMD, EVENT, STORE>>>,
    replica_id: ReplicaId,
    message: VoidCausalMessage<CMD, EVENT>,
)
    where C: CRDT<STATE, CMD, EVENT> + Clone + Unpin,
          STATE: Send + Unpin,
          CMD: Send + Unpin,
          EVENT: Send + Clone + Unpin,
          STORE: EventStore<C, STATE, CMD, EVENT> + Unpin
{
    replicas
        .get(&replica_id)
        .unwrap()
        .clone()
        .recipient()
        .do_send(message);
    //.expect(&*format!("The delivery of the message to replica {} failed!", replica_id));
}

async fn send_valued<C, STATE, CMD, EVENT, STORE>(
    replicas: &HashMap<ReplicaId, Addr<Replica<C, STATE, CMD, EVENT, STORE>>>,
    replica_id: ReplicaId,
    message: ValuedCausalMessage<STATE>,
) -> STATE
    where C: CRDT<STATE, CMD, EVENT> + Clone + Unpin,
          STATE: Send + Unpin,
          CMD: Send + Unpin,
          EVENT: Send + Clone + Unpin,
          STORE: EventStore<C, STATE, CMD, EVENT> + Unpin
{
    replicas
        .get(&replica_id)
        .unwrap()
        .clone()
        .send(message)
        .await
        .unwrap()
        .0
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
                LSeq::<char>::default(),
                InMemory::<LSeq<char>, Vec<char>, LSeqCommand<char>, LSeqOperation<char>>::create(),
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
                let replica_id: usize = match replica_id.trim().parse() {
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

                        let mut receiver = LSeqReceiver::new(replica_id);
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

struct LSeqReceiver {
    replica_id: ReplicaId,
    commands: Vec<LSeqCommand<char>>,
}

impl LSeqReceiver {
    fn new(replica_id: ReplicaId) -> LSeqReceiver {
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

// TODO:
// * Try a socket based implementation.
// * Implement more complex operation-based CRDTs.
fn main() {
    start();
}

