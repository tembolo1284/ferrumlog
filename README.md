# ferrumlog

A durable, replicated, append-only event log in Rust with a gRPC API.
Producers append records to named topics, and consumers read them back by
offset or follow them live. Three nodes keep a copy each and agree on its
contents through Raft, written from scratch. Think of a small Kafka.

**Status:** a three-node cluster works end to end on one machine: leader
election, replication, failover, and catching up after a restart. It has
not been deployed to AWS yet, and there are no performance numbers yet. See
the [roadmap](#roadmap).

## Quick start

You need a stable Rust toolchain and nothing else. The protobuf compiler is
bundled.

```bash
./build.sh run            # start a node on 127.0.0.1:7001, data in ./data
```

In a second terminal:

```bash
cargo install --path crates/fl-cli    # installs the `fl` command

fl create-topic quotes
fl produce quotes "UST10Y 99.5" "UST2Y 100.1"
fl consume quotes
fl consume quotes --from latest --follow    # in one terminal, then produce in another
```

Or let the script do all of it against a throwaway node:

```bash
./build.sh demo
```

```text
$ fl create-topic quotes
created topic quotes (id 1)

$ fl produce quotes "UST10Y 99.5" "UST2Y 100.1"
wrote 2 records to quotes, offsets 0 to 1

$ printf 'a\nb\nc\n' | fl produce quotes --key batch
wrote 3 records to quotes, offsets 2 to 4

$ fl consume quotes --follow --from 3 -n 2
3	batch	b
4	batch	c

$ fl status
ENDPOINT                NODE  ROLE    TERM  LEADER  LAST  COMMIT  APPLIED  SEGMENTS  SIZE
http://127.0.0.1:17001  1     leader  1     1       4     4       4        1         257 B
```

## A cluster on your laptop

```bash
./build.sh cluster        # three nodes on 127.0.0.1:7001 to 7003
```

In a second terminal:

```bash
export FERRUMLOG_ENDPOINTS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
fl status
fl create-topic quotes
fl produce quotes hello
```

```text
ENDPOINT               NODE  ROLE      TERM  LEADER  LAST  COMMIT  APPLIED  SEGMENTS  SIZE
http://127.0.0.1:7001  1     follower  1     2       1     1       1        1         41 B
http://127.0.0.1:7002  2     leader    1     2       1     1       1        1         41 B
http://127.0.0.1:7003  3     follower  1     2       1     1       1        1         41 B
```

Now kill the leader and write again. The other two elect a new leader in
about a second, and the client finds it on its own.

```bash
pgrep -af 'fl-server --node-id'     # find the leader's pid
kill -9 <pid>
fl produce quotes "still here"
fl status
```

Start the dead node again with the same flags and it rejoins as a follower
and catches up.

## What it guarantees today

- **An acknowledged write is on a majority of disks.** The leader answers a
  produce call only after a majority of nodes have fsynced the records. In
  a cluster of three, any one node can be lost without losing a write.
- **Readers never see a record that could be rolled back.** Only committed
  records are served.
- **Offsets are dense and stable.** The first record in a topic is offset 0,
  and a record keeps its offset forever, on every node.
- **The cluster keeps working while a majority is up.** A dead leader is
  replaced in about a second with the default settings.
- **A crash loses nothing that was acknowledged.** On restart the log is
  scanned, a half-written tail is cut off, and the node catches up from the
  leader.
- **Damage is detected, not hidden.** Every record carries a CRC-32C that is
  checked on every read.

What it does not do yet:

- **No TLS and no authentication**, for clients or between nodes. The
  defaults bind to loopback. Do not expose the ports to the internet.
- **No exactly-once delivery.** A write that fails with "outcome unknown",
  for example because the leader changed under it, may or may not have
  been committed. The client library does not send it again unless you ask
  it to, because that could write it twice.
- **Reads can be slightly behind.** Any node serves reads from what it has
  applied. A follower, or a leader that has just been replaced, can lag by
  a moment.
- **The cluster's members are fixed.** Nodes cannot be added or removed
  without restarting everything with a new peer list.
- **The log is never compacted.** A restarted node replays all of it.

## How it works

```text
 fl (CLI) --+                        +-- fl-raft: elections, replication
            +--> fl-client --gRPC--> fl-server --> fl-broker --> fl-storage
 your code -+                        |             topics and    segment files,
                                     |             offsets       CRC, fsync
                                     +-- fl-net <--gRPC--> the other nodes
```

**Replication.** A write goes to the leader, which appends it to its log
and sends it to the followers. Once a majority has it on disk it is
committed. Each node then applies it to its topic state, and the leader
answers the client. A node that is not the leader answers a write with a
redirect naming the leader, and the client library follows it.

**Storage.** The log is a directory of segment files. Each record batch is
one framed entry: a length, a CRC-32C, and the payload. Only the last
segment is written. When it reaches 64 MiB it is synced, an index file is
written beside it, and a new segment starts.

```text
data/
  00000000000000000001.log     sealed segment, named by its first log index
  00000000000000000001.idx     index for that segment
  00000000000000048213.log     active segment
```

**Topics.** Records are stored once, in the log. For each topic the broker
keeps a small in-memory index that maps offsets to log entries. It is
rebuilt by replaying the log on startup.

**Group commit.** Everything that changes a node goes through one queue to
one thread. The thread takes everything that is waiting, appends it, and
calls fsync once. Under load many writes share one fsync and one message
to each follower, so throughput grows with concurrency.

**Subscriptions.** A subscription streams the backlog and then waits for
new commits. A slow consumer slows only its own stream. The client library
reconnects after a dropped connection and resumes at the next offset.

## Raft and the simulator

`fl-raft` is Raft written from scratch as a pure state machine. It
implements leader election, log replication, and repair of diverged logs. It does no
I/O and reads no clock. It is handed ticks, messages, and proposals, and it
answers with what to persist, what to send, and how far the log is
committed.

That makes it possible to run a whole cluster inside one thread. `fl-sim`
does exactly that, and plays the network, the disks, and the clocks itself.
It drops, duplicates, delays, and reorders messages, partitions the
cluster, and crashes nodes in the middle of a step. After every step it
checks the safety properties from the Raft paper: one leader per term, the
same entry applied at each index on every node, and no committed entry
ever missing from a later leader. Every run is decided by its seed, so a
failure replays exactly.

```bash
./build.sh sim                                # 100 seeds, moderate faults
./build.sh sim --count 2000 --profile harsh
./build.sh sim --seed 282 --count 1 --trace   # replay one seed
```

## Crates

| Crate        | What it is                                                        | Status  |
|--------------|-------------------------------------------------------------------|---------|
| `fl-types`   | Ids, records, log entries, membership. No dependencies.           | done    |
| `fl-storage` | Segmented log, index files, fsync, crash recovery, hard state.    | done    |
| `fl-proto`   | Protobuf definitions and generated gRPC code.                     | done    |
| `fl-broker`  | The state machine: topics, offsets, produce, fetch.               | done    |
| `fl-server`  | The node binary: gRPC services, the Raft thread, metrics.         | done    |
| `fl-client`  | Client library: failover, leader redirects, resumable streams.    | done    |
| `fl-cli`     | The `fl` command.                                                 | done    |
| `fl-raft`    | Raft as a pure state machine with no I/O.                         | done    |
| `fl-sim`     | Deterministic simulator: seeded partitions, crashes, clock skew.  | done    |
| `fl-net`     | Raft over gRPC: disk log adapter, wire format, peer transport.    | done    |
| `fl-bench`   | Load generator and latency histograms.                            | done    |
| `fl-tier`    | Archives sealed segments to a directory or S3, and restores.      | done    |

## Running a node

A single node:

```bash
fl-server --data-dir /var/lib/ferrumlog --listen 0.0.0.0:7001
```

One member of a three-node cluster. Every node gets the same `--peers`
and its own `--node-id`:

```bash
fl-server --node-id 1 \
    --peers 1=10.0.1.11:7001,2=10.0.2.11:7001,3=10.0.3.11:7001 \
    --listen 0.0.0.0:7001 --data-dir /var/lib/ferrumlog
```

A node started without `--peers` is a cluster of one. It runs the same
Raft code, elects itself at once, and commits on its own.

Every flag can also be set with an environment variable.

| Flag                 | Environment variable         | Default          | Meaning                                         |
|----------------------|------------------------------|------------------|-------------------------------------------------|
| `--node-id`          | `FERRUMLOG_NODE_ID`          | `1`              | This node's id                                  |
| `--data-dir`         | `FERRUMLOG_DATA_DIR`         | `data`           | Where the log lives                             |
| `--listen`           | `FERRUMLOG_LISTEN`           | `127.0.0.1:7001` | gRPC address                                    |
| `--advertise`        | `FERRUMLOG_ADVERTISE`        | listen address   | Address reported to clients                     |
| `--peers`            | `FERRUMLOG_PEERS`            | none             | Every node as `id=host:port`, comma separated   |
| `--tick-ms`          | `FERRUMLOG_TICK_MS`          | `50`             | Raft tick. Elections take 10 to 20 ticks.       |
| `--metrics-listen`   | `FERRUMLOG_METRICS_LISTEN`   | `127.0.0.1:9101` | HTTP address for `/metrics` and `/healthz`      |
| `--no-metrics`       | `FERRUMLOG_NO_METRICS`       | off              | Do not start the HTTP server                    |
| `--segment-bytes`    | `FERRUMLOG_SEGMENT_BYTES`    | `67108864`       | Segment size before a new one starts            |
| `--max-commit-batch` | `FERRUMLOG_MAX_COMMIT_BATCH` | `256`            | Most writes committed with one fsync            |
| `--unsafe-no-fsync`  | `FERRUMLOG_UNSAFE_NO_FSYNC`  | off              | Skip fsync. Benchmarks only. Can lose data.     |

Set `RUST_LOG=debug` for more logging. The node stops cleanly on Ctrl-C or
SIGTERM: it finishes requests in flight, commits queued writes, and exits
with status 0. A storage error stops the node with a non-zero status.

Do not start two nodes on the same data directory. Nothing prevents it yet.

The address in `--peers` is each node's gRPC address. The other nodes use
it for Raft and clients are redirected to it, so both must be able to reach
it. The same port carries the client API and the Raft protocol. Open it to
the other members and to your clients, and to nothing else.

### Metrics

`GET /metrics` returns Prometheus text. `GET /healthz` returns 200 while the
node is serving and 503 once it is shutting down.

| Metric                                | Type      | Meaning                               |
|---------------------------------------|-----------|---------------------------------------|
| `ferrumlog_produce_requests_total`    | counter   | Produce calls, labelled by `result`   |
| `ferrumlog_records_produced_total`    | counter   | Records committed                     |
| `ferrumlog_bytes_produced_total`      | counter   | Key and value bytes committed         |
| `ferrumlog_produce_duration_seconds`  | histogram | Produce latency as the server sees it |
| `ferrumlog_fetch_requests_total`      | counter   | Fetches and subscription reads        |
| `ferrumlog_records_fetched_total`     | counter   | Records returned to consumers         |
| `ferrumlog_subscriptions`             | gauge     | Open subscribe streams                |
| `ferrumlog_commits_total`             | counter   | Group commits, one fsync each         |
| `ferrumlog_commit_batch_size`         | histogram | Writes per group commit               |
| `ferrumlog_commit_duration_seconds`   | histogram | Time to append and fsync one batch    |
| `ferrumlog_log_last_index`            | gauge     | Index of the last log entry           |
| `ferrumlog_log_bytes`                 | gauge     | Size of the segment files             |
| `ferrumlog_log_segments`              | gauge     | Number of segment files               |
| `ferrumlog_topics`                    | gauge     | Number of topics                      |
| `ferrumlog_raft_term`                 | gauge     | This node's current Raft term         |
| `ferrumlog_raft_leader`               | gauge     | 1 while this node leads, otherwise 0  |
| `ferrumlog_commit_index`              | gauge     | Highest committed log index           |

Dividing `ferrumlog_produce_requests_total` by `ferrumlog_commits_total`
gives the average number of writes that shared an fsync.

## Docker

```bash
./build.sh docker         # builds ferrumlog:local

docker run -d --name ferrumlog \
    -p 127.0.0.1:7001:7001 -p 127.0.0.1:9101:9101 \
    -v ferrumlog-data:/var/lib/ferrumlog \
    ferrumlog:local

docker exec ferrumlog fl status
docker stop ferrumlog     # sends SIGTERM, the node shuts down cleanly
```

The image runs as an unprivileged user with uid 10001. If you mount a host
directory instead of a named volume, give that user ownership of it first:
`sudo chown 10001 /path/to/data`.

The `-p 127.0.0.1:...` form publishes the ports on loopback only. Drop the
`127.0.0.1:` prefix to reach the node from another machine, and only do that
behind a firewall that limits who can connect.

## The `fl` command

```text
fl topics                               list topics
fl create-topic <name>                  create a topic
fl produce <topic> [values...]          append records, or read lines from stdin
fl consume <topic>                      print records and stop at the end
    --from earliest|latest|<offset>     where to start (default earliest)
    --follow                            keep printing new records
    -n <count>                          stop after this many
fl status                               show every node
```

`fl` reads the node addresses from `--endpoints` or from
`FERRUMLOG_ENDPOINTS`, as a comma-separated list. Output from `consume` is
offset, key, and value separated by tabs, with `-` for a record that has no
key.

## Using the client library

```rust
use fl_client::{Client, ClientConfig, StartAt};
use fl_types::Record;

#[tokio::main]
async fn main() -> Result<(), fl_client::Error> {
    let client = Client::connect(ClientConfig::new(["127.0.0.1:7001"])).await?;

    client.create_topic("quotes").await?;
    let produced = client
        .produce("quotes", vec![Record::new("99.5").with_key("UST10Y")])
        .await?;
    println!("written at offset {}", produced.base_offset);

    let mut subscription = client.subscribe("quotes", StartAt::Earliest).await?;
    loop {
        let batch = subscription.next().await?;
        for consumed in batch.records {
            println!("{} {:?}", consumed.offset, consumed.record.value);
        }
    }
}
```

## Development

```bash
./build.sh check     # formatting, clippy with warnings as errors, all tests
./build.sh test      # tests only
./build.sh fmt       # format in place
./build.sh help      # everything else
```

CI runs `./build.sh check`'s steps on every push and pull request, then
builds the Docker image, starts it, writes and reads through it, and checks
that it stops cleanly.

Layout:

```text
crates/              the Rust workspace, one directory per crate
deploy/docker/       Dockerfile
.github/workflows/   CI
build.sh             build, check, run, demo, cluster, sim, bench
```

## Roadmap

- [x] Durable segmented log with crash recovery
- [x] Topics, offsets, produce, fetch, subscribe
- [x] gRPC server with group commit, metrics, graceful shutdown
- [x] Client library and CLI
- [x] Raft: leader election and log replication
- [x] Deterministic simulation testing of Raft
- [x] Raft transport, and the server running as a replicated cluster
- [ ] Raft snapshots, membership changes, and pre-vote
- [ ] Three-node cluster on AWS, provisioned with Terraform
- [x] Load generator
- [ ] Published throughput and latency numbers from real machines
- [x] Archiving sealed segments to S3, with verified restore
- [ ] True tiering: dropping local segments that are in S3
- [ ] TLS and authentication
- [ ] Idempotent producer, so retried writes cannot duplicate

