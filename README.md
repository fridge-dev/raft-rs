# raft-rs

Implementation of https://raft.github.io/raft.pdf.

Current status: Developmental (don't use)

# Misc dev notes

Experiment: I'm using 3 different styles of giving a primitive value a more explicit type.
I use the following, and I plan to see while working on this project what is easiest to use.

**Type alias** in `replica/replica.rs`:
```rust
pub type Term = u64;
```

**Wrapper struct - private inner** in `commitlog/log.rs`:
```rust
pub struct Index(u64);
impl Index { /* accessors */ }
```

**Wrapper struct - private inner** in `replica/peers.rs`:
```rust
pub struct ReplicaId(pub String);
```
