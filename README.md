## Stream shard groups
This is the nodes for the streams responsible for writing and reading to partitions. Each shard group can have many partitions and is replicated using raft. Partitions do not yet exist between multiple shard groups as the  [control plane](https://github.com/Kapperchino/Stream-Infra) needs to be implemented.
## Current Progress
- Reading from a partition(Done, needs rewrite)
- Writing to a partition(Done)
- Client(Done)
- Snapshot(Done)
- Streaming from partition to client(TBD)
- Streaming from client to partition(TBD)
## Docs
Docs will be in the doc folder
## Contributions
Any help is appreciated 
