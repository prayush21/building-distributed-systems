# Building Memcache

This project is a step-by-step implementation of a distributed key-value store inspired by Memcached, built in Go. The goal is to create a scalable, high-performance caching system from scratch.

## Project Checklist

- [x] Milestone 1: Setting up Single-Node Key-Value Store
- [x] Milestone 2: Sharding
- [ ] Milestone 3: Replication
- [ ] TODO

## Milestones

### Milestone 1: Setting up Single-Node Key-Value Store ✅ (Completed)

Steps (referencing memcache/main.go):

- Set up TCP listener and handle connections (listener.Accept, handleConnection).
- Parse incoming commands (strings.Fields, switch on command).
- Implement thread-safe Get/Set using mutex on map (Store struct with sync.RWMutex).

### Milestone 2: Sharding and setting up a Distributed Cache Client (using Consistent Hashing)

Steps (referencing main.go):

- Set up Client interface over the net.Conn interface, with additional operations for GET and SET to the store at server.
- Implement the Consistent Hashing. Setup Ring interface with AddNode & GetNode.
- Bring together Client and Ring Interface to get a Distributed Cache Client.
- Add validation & error handling (Let CC handle this).

### Next Phase: Replication
