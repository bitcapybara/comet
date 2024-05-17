# Comet MQ

[中文文档](./README-zh.md)

A message queue library inspired by Pulsar, including both server and client components.

## Key Features

* Custom communication protocol for handling packet assembly/disassembly, minimizing message size.
* Server protocol utilizes the [serde](https://docs.rs/serde/latest/serde/) library for serialization/deserialization. Currently supports [bincode](https://docs.rs/bincode/latest/bincode/) and [json](https://docs.rs/serde_json/latest/serde_json/), with the ability for users to specify custom formats.
* Uses [`QUIC`](https://github.com/aws/s2n-quic) for client-server protocol communication.
    * Employs a `stream` pool to avoid frequent creation.
    * Implements a `pipeline` mode for sending messages, enhancing message transmission concurrency.
* Ensures secure message transmission with `mutual TLS` authentication.
* Server supports three `features` to adapt to different deployment scenarios:
    * Local Memory Mode (`local-memory`): Metadata and messages are stored in the memory of the current node, suitable for one-time testing.
    * Local Persistence Mode (`local-persist`): Metadata is stored in [`redb`](https://github.com/cberner/redb) files, and messages are stored in [`sqlite`](https://www.sqlite.org/index.html) files, suitable for single-node persistence in non-distributed scenarios.
    * Distributed Mode (`distributed`): Metadata is stored in [`etcd`](https://etcd.io/), and messages are stored in [`PostgreSQL`](https://www.postgresql.org/), suitable for large-scale distributed deployments.
* The server is stateless, allowing nodes to be started and stopped freely. Clients will automatically migrate to other nodes if the server connection is lost.
* Supports both `immediate messages` and `delayed messages`. Client `Producers` can specify the delivery time for the server to send the message to consumers.
* Supports specifying the retention policy for acknowledged messages based on `quantity/TTL`.
* Topic supports `partitioning`, allowing client producers to round-robin messages across multiple topic partitions on different nodes, enhancing message sending concurrency.
* Client `Producer` implements the [`Sink trait`](https://docs.rs/futures/latest/futures/sink/trait.Sink.html), and `Consumer` implements the [`Stream trait`](https://docs.rs/futures/latest/futures/stream/index.html), facilitating usage in streaming scenarios.

## Example

1. Install [mkcert](https://github.com/FiloSottile/mkcert)

2. Generate cert authentication files

```bash
make init
```

3. Start the client

```bash
cargo run --example client
```

4. Start the server

```bash
# local-memory mode
cargo run --example server --features local-memory

# local-persist mode
cargo run --example server --features local-persist

# distributed mode
cargo run --example server --features distributed
```