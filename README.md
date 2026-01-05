# Building a Memcache Clone

This project is a step-by-step implementation of a distributed key-value store inspired by Memcached, built in Go. The goal is to create a scalable, high-performance caching system from scratch.

## Project Checklist

- [x] Milestone 1: Setting up Single-Node Key-Value Store
- [ ] Milestone 2: Sharding
- [ ] TODO
- [ ] TODO

## Milestones

### Milestone 1: Setting up Single-Node Key-Value Store ✅ (Completed)

Steps (referencing main.go):

- Set up TCP listener and handle connections (listener.Accept, handleConnection).
- Parse incoming commands (strings.Fields, switch on command).
- Implement thread-safe Get/Set using mutex on map (Store struct with sync.RWMutex).
- Add validation, error handling, and responses (error messages for invalid commands).

### Next Phase: Sharding
