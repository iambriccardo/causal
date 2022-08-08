# Causal

Causal is a Reliable Causal Broadcast (RCB) protocol implemented in **Rust** and **Actix**.

The protocol is an adaptation from [this article](https://bartoszsypytkowski.com/operation-based-crdts-protocol) which
is a great read for anyone wanting to try and learn more about operation-based CRDTs.

## What are CmRDTs?
Operation-based CRDTs are also called commutative replicated data types, or CmRDTs. CmRDT replicas propagate state by transmitting only the update operation.
Replicas receive the updates and apply them locally.

The operations are **commutative**. However, they are **not necessarily idempotent**. The communications infrastructure must therefore ensure that all operations on a replica are delivered to the other replicas, without duplication, but in any order.

## Implemented CmRDTs

- ORSet
- LSeq _(WIP)_
- RGA _(TODO)_

## Disclaimer

_Please note that the work on this protocol has just been started and there are for sure bugs and features that must be
solved/added._