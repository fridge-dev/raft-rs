# raft-rs

Implementation of https://raft.github.io/raft.pdf.

Current status: Developmental (don't use)

# Features

Supported:

* Log replication

Planned:

1. Cluster membership changes
1. Better fault testing, similar to [etcd](https://github.com/etcd-io/etcd/blob/main/tests/functional/rpcpb/rpc.pb.go#L207-L518)
1. Log compaction/snapshotting
1. Durability (currently does all "persistence" in memory)
