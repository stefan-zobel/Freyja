# Using Freyja

How to build Freyja, start a cluster, read and write data, and configure a node. What to do
when something goes wrong is in the runbook (`docs/runbook.md`); what every part does
exactly is in `docs/spec/`.

Freyja is an educational system. It is not released yet and has never run on a real
cluster of several machines.

## 1. What It Is and What It Is Not

- A cluster of usually three or five nodes. Every node holds all of the data; one Raft group
  replicates every write. A write is acknowledged when a majority of the nodes has it on
  disk, so a cluster of three keeps working when one node is down.
- Keys and values are byte strings. Clients can put, delete, read, and scan, write several
  keys at once, and write on a condition (compare and set).
- Not there: sharding (the data must fit on every node), transactions across several
  requests, watches, keys that expire, users and roles. Clients authenticate with a
  certificate or a token, and every authenticated client may do everything.

## 2. Build

You need JDK 25. Every program runs with `--enable-preview`.

```
git clone <the repository> freyja && cd freyja
./mvnw -B package -DskipTests
mkdir -p ~/freyja-lib
cp */target/*-0.0.1-SNAPSHOT.jar ~/freyja-lib/
```

The examples below use these shell functions:

```
FREYJA_CP="$HOME/freyja-lib/*"
freyja-node()  { java --enable-preview -XX:+UseZGC -cp "$FREYJA_CP" io.github.sz.freyja.node.FreyjaNode "$@"; }
freyja-admin() { java --enable-preview -cp "$FREYJA_CP" io.github.sz.freyja.tools.FreyjaAdmin "$@"; }
freyja-cli()   { java --enable-preview -cp "$FREYJA_CP" io.github.sz.freyja.tools.FreyjaCli "$@"; }
```

On Windows, `mvnw.cmd` builds, and the commands work the same in Git Bash.

## 3. Quick Start: Three Nodes on One Machine

The outputs below are from a run on Linux. The cluster runs without TLS (`--insecure yes`):
anyone who can reach its ports can do anything. Use that on your own machine only.

**Prepare a data directory for every node.** A cluster has an id (hexadecimal, here `1f`)
and every node a number. The three nodes are the voters from the start:

```
$ for n in 0 1 2; do freyja-node init --data data/n$n --cluster 1f --node $n --voters 0,1,2; done
prepared data/n0 for node 0 of cluster 1f, voters [0, 1, 2]
prepared data/n1 for node 1 of cluster 1f, voters [0, 1, 2]
prepared data/n2 for node 2 of cluster 1f, voters [0, 1, 2]
```

**Start the nodes**, each in a terminal of its own (or with `&`). Every node listens on four
ports: for the other nodes (`--peer`), for clients (`--client`), for the admin tool
(`--admin`), and for metrics and health over HTTP (`--status`). `--seed` tells it where the
nodes are when it starts for the first time:

```
$ freyja-node run --data data/n0 --peer 127.0.0.1:7100 --client 127.0.0.1:7200 \
    --admin 127.0.0.1:7300 --status 127.0.0.1:7400 \
    --seed 0=127.0.0.1:7100 --seed 1=127.0.0.1:7101 --seed 2=127.0.0.1:7102 --insecure yes
WARNING: node 0 runs in INSECURE mode: no TLS, and whoever reaches a port is taken for what it says it is
READY node=0 peer=7100 client=7200 admin=7300 status=7400
INFO: node 0 is LEADER in term 1, leader node 0
```

Node 1 gets the ports 7101, 7201, 7301, 7401, node 2 the ports 7102, 7202, 7302, 7402, and
the same three `--seed` options. Within a second or two one of the nodes is the leader. (The
log lines above are shortened: each comes with a line that names the time and the class.)

**Look at the cluster:**

```
$ freyja-admin 127.0.0.1:7300 status
node:            0 of cluster 1f, software 0.1.0
role:            LEADER in term 1, leader is node 0
log:             last 1, committed 1, applied 1, starts after 0
cluster version: 1
alarms:          none
restarts:        0, snapshots installed 0
member:          node 0 voter, log agrees through 1, ok
member:          node 1 voter, log agrees through 1, ok
member:          node 2 voter, log agrees through 1, ok
```

**Tell the cluster where its nodes listen.** A node that is not the leader sends a client
to the leader by the address the cluster knows; a new cluster knows none yet. Ask the
leader (here node 0):

```
$ freyja-admin 127.0.0.1:7300 set-address 0 127.0.0.1:7100 127.0.0.1:7200
the address of node 0 is applied, at index 2
$ freyja-admin 127.0.0.1:7300 set-address 1 127.0.0.1:7101 127.0.0.1:7201
the address of node 1 is applied, at index 3
$ freyja-admin 127.0.0.1:7300 set-address 2 127.0.0.1:7102 127.0.0.1:7202
the address of node 2 is applied, at index 4
```

**Write and read.** `freyja-cli` takes the client ports of the nodes; one that answers is
enough, and it finds the leader itself:

```
$ NODES=127.0.0.1:7200,127.0.0.1:7201,127.0.0.1:7202
$ freyja-cli $NODES put greeting "hello, Freyja"
ok
$ freyja-cli $NODES get greeting
hello, Freyja
$ freyja-cli $NODES put fruit/apple red
ok
$ freyja-cli $NODES put fruit/banana yellow
ok
$ freyja-cli $NODES put fruit/cherry red
ok
$ freyja-cli $NODES scan fruit/ fruit0
fruit/apple	red
fruit/banana	yellow
fruit/cherry	red
$ freyja-cli $NODES delete fruit/banana
ok
$ freyja-cli $NODES get fruit/banana
no value
```

A scan runs from its first key (inclusive) to its second (exclusive); `fruit0` is the first
key after every key that starts with `fruit/`. Without keys it scans everything.

**Stop the leader** (Ctrl-C in its terminal, or `kill`). The other two elect a new leader,
and the data is still there:

```
$ freyja-cli $NODES get greeting
hello, Freyja
$ freyja-admin 127.0.0.1:7301 status
node:            1 of cluster 1f, software 0.1.0
role:            FOLLOWER in term 2, leader is node 2
...
$ freyja-cli $NODES put after "the leader stopped"
ok
```

**Start it again** with the same command. It catches up from the others:

```
$ freyja-cli 127.0.0.1:7200 get after --stale
the leader stopped
```

`--stale` reads from the node that is asked, without asking the leader whether it has the
latest value; here it shows that node 0 has caught up. A node stops cleanly on `SIGTERM`
(Ctrl-C), and it may also be killed any other way: it recovers from its files.

## 4. The Command-Line Tool

```
freyja-cli [OPTION]... HOST:PORT[,HOST:PORT]... COMMAND [ARGUMENT]...

put KEY VALUE                                set the value of a key
get KEY [--stale]                            print the value of a key
delete KEY                                   remove a key
scan [START [END]] [--limit N] [--stale]     print KEY<tab>VALUE per line, at most N (100)

--hex                       keys and values in hexadecimal instead of UTF-8 text
--timeout-ms N              how long the command may take (10,000)
--tls-trust FILE            for a cluster with TLS: the authorities of the node certificates
--tls-key FILE              the certificate of the user, or instead:
--token-file FILE           a token in the first line of the file
--tls-password-file FILE    the password of the key and trust stores
--tls-name ATTRIBUTE=VALUE  what the certificate of a node must carry, for example OU=freyja-peer
```

Exit code 0: done. 1: the cluster could not be reached, refused, or did not answer in time.
2: wrong usage. 3: `get` found no value. If a write ran out of time after it was sent, the
tool says that its outcome is not known: it may have taken effect.

Every call opens a session with the cluster and closes it again; that costs two entries in
the log. For more than a few calls, use the library.

## 5. Using It From Java

The client library is the module `kv-client`; it needs only `kv-common`. Install the modules
into your local Maven repository with `./mvnw -B install -DskipTests` and add:

```
<dependency>
    <groupId>io.github.stefan-zobel</groupId>
    <artifactId>kv-client</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Compile and run your program with `--enable-preview`. An example that was run against the
quick-start cluster:

```java
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.client.FreyjaException;
import io.github.sz.freyja.common.kv.Command.Condition;
import io.github.sz.freyja.common.kv.Command.Delete;
import io.github.sz.freyja.common.kv.Command.Put;

public class Example {

    static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] args) {
        FreyjaClient.Options options = FreyjaClient.Options.of("127.0.0.1:7200", "127.0.0.1:7201", "127.0.0.1:7202");
        try (FreyjaClient client = FreyjaClient.open(options)) {
            client.put(b("greeting"), b("hello, Freyja"));

            Optional<FreyjaClient.Versioned> value = client.get(b("greeting"));
            value.ifPresent(v -> System.out.println(new String(v.value(), StandardCharsets.UTF_8)
                    + " (written at index " + v.modIndex() + ")"));

            // Several keys at once: all of them or none.
            client.write(List.of(new Put(b("fruit/apple"), b("red")), new Put(b("fruit/cherry"), b("red")),
                    new Delete(b("fruit/banana"))));

            // Compare and set: change the value only if nobody has written it since we read it.
            long seen = client.get(b("greeting")).orElseThrow().modIndex();
            boolean changed = client.putIfUnchanged(b("greeting"), b("hello again"), seen);
            System.out.println("changed: " + changed);

            // A conditional batch: if the key does not exist, create it; otherwise delete it.
            boolean held = client.conditional(List.of(Condition.notExists(b("lock"))),
                    List.of(new Put(b("lock"), b("mine"))), List.of(new Delete(b("lock"))));
            System.out.println("the lock was free: " + held);

            for (FreyjaClient.Entry entry : client.scan(b("fruit/"), b("fruit0"), 100)) {
                System.out.println(new String(entry.key(), StandardCharsets.UTF_8) + " = "
                        + new String(entry.value(), StandardCharsets.UTF_8));
            }
        } catch (FreyjaException.UnknownOutcome e) {
            // The write may or may not have taken effect: read before you try again.
            System.err.println("unknown outcome: " + e.getMessage());
        }
    }
}
```

```
hello, Freyja (written at index 26)
changed: true
the lock was free: true
fruit/apple = red
fruit/cherry = red
```

What to know about the client:

- **Every read is linearizable** unless it is a `...Stale` method: it sees every write that
  was acknowledged before it began. A stale read is answered by the first node that answers
  and may be behind.
- **Every write takes effect once**, even if the client has to send it again after a lost
  connection or a change of the leader; the session of the client makes that so.
- **An unknown outcome is not a failure.** If the time of a write runs out after it was
  sent, the client cannot know whether it took effect and throws
  `FreyjaException.UnknownOutcome`. Read, then decide. Every other `FreyjaException` means
  that the request had no effect: `TimedOut` (a read, or a write no node got), `Overloaded`,
  `NoSpace` (the cluster takes no more writes, §8), `Refused`.
- **One request at a time per client.** Calls from several threads take turns. For requests
  in parallel, open several clients.
- **Settings** (`FreyjaClient.Options`): `withRequestTimeout` (10 s), `withKeepAlive` (an
  idle client tells the cluster every 10 s that it is alive, so that its session does not
  expire after 60 s), `withTls` for a cluster with TLS, `withRetryWhenOverloaded`.
- `close()` ends the session.

## 6. Configuring a Node

**`FreyjaNode init`** prepares a data directory once:

| Option | Meaning |
|---|---|
| `--data DIR` | The data directory; it must not hold a node yet. |
| `--cluster HEX` | The id of the cluster, the same on every node. |
| `--node N` | The number of this node, used once in the life of a cluster. |
| `--voters N,N,...` | The voters a new cluster starts with. Without it the node is prepared to join an existing cluster (runbook §2). |

**`FreyjaNode run`** runs a node from its directory:

| Option | Default | Meaning |
|---|---|---|
| `--data DIR` | | The data directory. |
| `--peer HOST:PORT` | | Where the other nodes reach it. |
| `--client HOST:PORT` | | Where clients reach it. |
| `--admin HOST:PORT` | | Where the admin tool reaches it. |
| `--status HOST:PORT` | none | Metrics and health over HTTP (§7). |
| `--seed N=HOST:PORT` | | Where node N listens for peers; once per node, used until the cluster knows the addresses itself. |
| `--tick-ms N` | 100 | The length of a tick. A leader sends a heartbeat every tick; an election starts after 10 to 20 ticks without one. |
| `--segment-bytes N` | 64 MiB | The size of a file of the Raft log. |
| `--snapshot-log-bytes N` | 1 GiB | The size of the log at which the node takes a snapshot and shortens its log. |
| `--snapshot-retain N` | 10,000 | How many applied entries it keeps then, for followers that are a little behind. |
| `--quota-bytes N` | 16 GiB | The size of the data at which the cluster stops taking writes (§8). |
| `--min-free-bytes N` | 1 GiB | The free disk space below which it stops taking writes. |
| `--insecure yes` | | No TLS, no authentication. |
| `--tls-key`, `--tls-password-file`, `--peer-trust`, `--peer-name`, `--client-trust`, `--client-name`, `--client-tokens`, `--admin-trust`, `--admin-name` | | TLS on all three listeners (§9). |

Sizes are plain numbers of bytes. A node needs either `--insecure yes` or the TLS options.

**The JVM.** Run a node with ZGC and a heap of fixed size, for example
`-XX:+UseZGC -Xms4g -Xmx4g` as in the runbook; with G1 the pauses of the collector were most
of the slow answers in the measurements (`docs/spec/node.md` §1). The heap holds the
memtables (32 MiB each), a block cache of 256 MiB, and what waits to be written and sent;
the data itself is on disk. The whole heap is resident from the start: leave memory beside
it for the file cache of the operating system.

**The disk.** A node syncs every write to disk before it acknowledges it, so the latency of
the disk is the latency of a write. Use an SSD. A node keeps a reserve file of 256 MiB that it
gives up when the disk is full, so that it can still work its way out.

## 7. Watching a Node

With `--status`, a node answers over HTTP:

- `/health`: `ok` while the node runs; otherwise why not, for example that it has ended for
  good and why. The reason says what to do (runbook §3, §5).
- `/ready`: `ok` if the node can serve clients; otherwise why not, for example that it
  knows no leader or is far behind.
- `/metrics`: every figure as `name value` lines, for a scraper such as Prometheus. A few
  to watch:
  - `freyja_raft_role` (0 follower, 3 leader) and `freyja_raft_term`: a term that keeps
    rising means elections that keep failing;
  - `freyja_raft_follower_N_lag` on the leader: how far node N is behind;
  - `freyja_client_command_p99_micros` and `..._p999_micros`: how long writes take;
  - `freyja_node_log_sync_max_micros`: the slowest sync of the disk;
  - `freyja_alarm_nospace`, `freyja_disk_free_bytes`, `freyja_node_stored_bytes`;
  - `freyja_node_restarts`: how often the node stopped after a failure and started again.

The full list is in `docs/spec/operations.md` §2. Every change of role, of the members, and
every alarm is also logged through `System.Logger` (`java.util.logging` by default).

## 8. When the Disk Fills Up

When the data reaches `--quota-bytes`, or the free space of a node falls below
`--min-free-bytes`, the cluster raises an alarm and refuses writes with `NoSpace`; reads go
on. Delete data or make room, then clear the alarm: runbook §4.

## 9. A Cluster on Several Machines

- One node per machine, three or five machines. Each node listens on addresses the others
  can reach; the client and admin ports are the ones your clients and you need.
- Run it with TLS. Every node gets a certificate for its name or address, clients a
  certificate or a token, administrators a certificate. How to make them with `keytool`:
  runbook Appendix A; what is checked: `docs/spec/operations.md` §5.
- Run the node as a service so that it is started again: the systemd unit is in runbook §0.
- Bootstrap: runbook §1 (the same steps as the quick start, with TLS and real addresses).

## 10. When Something Goes Wrong

| Situation | Where |
|---|---|
| A node is dead and does not come back | runbook §2 |
| A node reports a damaged disk or a lost identity | runbook §3 |
| Writes are refused with `NoSpace` | runbook §4 |
| A new release | runbook §5 |
| Backup and restore | runbook §6 |
| A majority of the nodes is lost for good | runbook §7 |
| Certificates expire | runbook §8 |

## 11. How Fast

On the development machine (one laptop, three nodes under WSL2, one NVMe SSD), a cluster
answered 4,000 writes per second with a median of 12 ms and a p99 of about 31 ms, and was
then about as fast as etcd 3.7.3 on the same machine (`docs/spec/verification.md` §7). The
disk is what limits it. These numbers are from one machine and are no promise for yours.
