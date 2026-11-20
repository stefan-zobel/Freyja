# Freyja Runbook

The procedures an operator follows, as numbered steps with the commands and what they
print. They are the procedures of `docs/spec/operations.md` §8.

**State of the procedures.** Every procedure except the rolling upgrade was carried out on
2026-11-19 with real node processes on one development machine (Windows 10, nodes on
`127.0.0.1`), and the outputs below are taken from those runs with the addresses replaced.
None has been carried out on a real cluster of several machines yet; that is required
before a release (`docs/spec/verification.md` §8). The rolling upgrade needs a second
release; only its steps that work within one release were carried out.

## 0. Conventions

**The software.** Build it once and put every module jar into one directory:

```
./mvnw -B package -DskipTests
mkdir -p /opt/freyja/lib
cp */target/*-0.0.1-SNAPSHOT.jar /opt/freyja/lib/
```

Every program needs JDK 25 and `--enable-preview`. The shell functions used below:

```
FREYJA_CP='/opt/freyja/lib/*'
# For a cluster with TLS (Appendix A); empty for a cluster in insecure mode.
FREYJA_ADMIN_TLS="--tls-key /etc/freyja/admin.p12 --tls-trust /etc/freyja/trust.p12 \
  --tls-password-file /etc/freyja/password --tls-name OU=freyja-peer"

freyja-node()   { java --enable-preview -cp "$FREYJA_CP" io.github.sz.freyja.node.FreyjaNode "$@"; }
freyja-admin()  { java --enable-preview -cp "$FREYJA_CP" io.github.sz.freyja.tools.FreyjaAdmin $FREYJA_ADMIN_TLS "$@"; }
node-verify()   { java --enable-preview -cp "$FREYJA_CP" io.github.sz.freyja.tools.NodeVerify "$@"; }
backup-verify() { java --enable-preview -cp "$FREYJA_CP" io.github.sz.freyja.tools.BackupVerify "$@"; }
freyja-bench()  { java --enable-preview -XX:+UseZGC -cp "$FREYJA_CP" io.github.sz.freyja.bench.FreyjaBench "$@"; }
```

**The example cluster.** Cluster id `1f`. Node `N` runs on `10.0.0.1N` and listens for peers
on port 7100, for clients on 7200, for administrators on 7300, and serves its metrics and
health on 7400. Its data directory is `/var/lib/freyja`. `I` in an output stands for a log
index, which differs from run to run.

**A node as a service** (systemd, one unit per machine; the example is node 0):

```
[Unit]
Description=Freyja node
After=network-online.target

[Service]
User=freyja
ExecStart=/usr/bin/java --enable-preview -XX:+UseZGC -Xms4g -Xmx4g -cp '/opt/freyja/lib/*' io.github.sz.freyja.node.FreyjaNode run \
  --data /var/lib/freyja \
  --peer 10.0.0.10:7100 --client 10.0.0.10:7200 --admin 10.0.0.10:7300 --status 10.0.0.10:7400 \
  --seed 0=10.0.0.10:7100 --seed 1=10.0.0.11:7100 --seed 2=10.0.0.12:7100 \
  --tls-key /etc/freyja/node.p12 --tls-password-file /etc/freyja/password \
  --peer-trust /etc/freyja/trust.p12 --peer-name OU=freyja-peer \
  --client-trust /etc/freyja/trust.p12 --client-name OU=freyja-client \
  --admin-trust /etc/freyja/trust.p12 --admin-name OU=freyja-admin
KillSignal=SIGTERM
TimeoutStopSec=30
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

A node runs with ZGC and a heap of fixed size (`docs/spec/node.md` §1). The whole heap is
resident from the start: the machine needs that much memory besides what the operating
system keeps for its file cache.

A node stops on `SIGTERM` and closes its files. It is safe to kill it any other way: it
recovers from its files at the next start.

**Where to look.**

- `freyja-admin HOST:7300 status`: role, term, leader, log (last, committed, applied, the
  index the log starts after), cluster version, alarms, and the members. Only the leader
  knows how far each member is (`log agrees through I`) and how it is (`ok`, `silent`,
  `recovering`, `snapshot`).
- `curl -s http://HOST:7400/ready`: `ok`, or the reason why the node cannot serve, for
  example `the node knows no leader`. `/health` says whether the node still runs at all.
- `curl -s http://HOST:7400/metrics`: every figure (`docs/spec/operations.md` §2).
- The log of the node (`System.Logger` `freyja.node`): every change of role, configuration,
  alarm, and repair, once.
- Changes of the cluster go to the leader. A node that is not the leader says
  `refused: ... this node is not the leader (the leader is node L)`; ask node `L`.

## 1. Bootstrap a Cluster

Three machines with the software, their certificates (Appendix A), and the service unit.

1. On every machine, prepare the data directory with the same cluster id and voters:

   ```
   freyja-node init --data /var/lib/freyja --cluster 1f --node 0 --voters 0,1,2
   ```
   ```
   prepared /var/lib/freyja for node 0 of cluster 1f, voters [0, 1, 2]
   ```
   On the other two machines with `--node 1` and `--node 2`.

2. Start the service on every machine (`systemctl start freyja`). Its log shows:

   ```
   READY node=0 peer=7100 client=7200 admin=7300 status=7400
   ```

3. Ask any node; within a few seconds one is the leader:

   ```
   freyja-admin 10.0.0.10:7300 status
   ```
   ```
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

4. Tell the cluster where its members listen. Until then a node cannot send a client to
   the leader by its address. At the leader:

   ```
   freyja-admin 10.0.0.10:7300 set-address 0 10.0.0.10:7100 10.0.0.10:7200
   freyja-admin 10.0.0.10:7300 set-address 1 10.0.0.11:7100 10.0.0.11:7200
   freyja-admin 10.0.0.10:7300 set-address 2 10.0.0.12:7100 10.0.0.12:7200
   ```
   ```
   the address of node 0 is applied, at index I
   ```

5. Check: every member has its addresses, and every node is ready.

   ```
   freyja-admin 10.0.0.10:7300 status | grep member
   ```
   ```
   member:          node 0 voter, peers at 10.0.0.10:7100, clients at 10.0.0.10:7200, log agrees through I, ok
   member:          node 1 voter, peers at 10.0.0.11:7100, clients at 10.0.0.11:7200, log agrees through I, ok
   member:          node 2 voter, peers at 10.0.0.12:7100, clients at 10.0.0.12:7200, log agrees through I, ok
   ```
   ```
   curl -s http://10.0.0.11:7400/ready
   ```
   ```
   ok
   ```

6. A cluster in insecure mode (a test installation only) can be tried with load; the
   benchmark talks to such clusters only:

   ```
   freyja-bench --endpoints 10.0.0.10:7200,10.0.0.11:7200,10.0.0.12:7200 --workload mixed --rate 200 --seconds 5
   ```
   It must report `completed = 1000` and `failed = 0`.

## 2. Replace a Dead Node

For a node whose machine or disk is gone for good. The node is replaced by a new one with a
new id; an id is never used twice. The new node is added before the dead one is removed, so
that the cluster never has fewer healthy voters than before.

1. Confirm at the leader that the node is not heard from:

   ```
   freyja-admin 10.0.0.10:7300 status | grep member
   ```
   ```
   member:          node 2 voter, peers at 10.0.0.12:7100, clients at 10.0.0.12:7200, log agrees through I, silent
   ```

2. On the new machine (here `10.0.0.13`), prepare a node with a new id and no voters:

   ```
   freyja-node init --data /var/lib/freyja --cluster 1f --node 3
   ```
   ```
   prepared /var/lib/freyja for node 3 of cluster 1f, to join a cluster
   ```

3. Start it with its own addresses and the members as seeds (the service unit with
   `--peer 10.0.0.13:7100` and so on). It prints `READY node=3 ...` and waits.

4. Add it as a learner, at the leader:

   ```
   freyja-admin 10.0.0.10:7300 add-learner 3 10.0.0.13:7100 10.0.0.13:7200
   ```
   ```
   node 3 as a learner is in the log at index I
   ```

5. Wait until the leader reports it `ok` with its log close to the last index. While it
   receives a snapshot it shows `snapshot`; that can take a while for a large state machine.

   ```
   member:          node 3 learner, peers at 10.0.0.13:7100, clients at 10.0.0.13:7200, log agrees through I, ok
   ```

6. Make it a voter, then remove the dead node:

   ```
   freyja-admin 10.0.0.10:7300 promote 3
   freyja-admin 10.0.0.10:7300 remove 2
   ```
   ```
   the promotion of node 3 is in the log at index I
   the removal of node 2 is in the log at index I
   ```

7. Check the status: three voters, all `ok`. Change the endpoint lists of clients and the
   monitoring. The machine of the dead node, should it come back, must not start its node
   again.

If a promotion or removal would leave fewer healthy voters than a majority, it is refused:
`refused: ... was refused: of the N voters there would be then, only W are well, and Q are
needed; force it if it must be`. Wait until the members are `ok`. `--force` skips the check
and is meant for a cluster that cannot recover otherwise.

## 3. A Node With a Damaged Disk or a Lost Identity

1. Stop the node (`systemctl stop freyja`).

2. Verify its data directory. The tool changes nothing:

   ```
   node-verify /var/lib/freyja
   ```

3. Act on what it says.

   - `verdict:      nothing is damaged`: start the node again.
   - Damage the node repairs itself, for example:

     ```
     raft log:     0000000001.rlog is DAMAGED
                   error: corruption in /var/lib/freyja/raft/0000000001.rlog at offset 4816: fragment checksum mismatch; the sync point at 5032 proves that this range was synced
     raft log:     1 segments, 1 damaged; the node will refill its log from the leader
     state machine: generation 1 is intact
     verdict:      DAMAGE, see above
     ```
     Start the node. It refills its log from the leader (it is `recovering` in the status of
     the leader and grants no votes meanwhile) or rebuilds its state machine from a snapshot
     of a healthy peer (`snapshot`). Wait until the leader reports it `ok` and its `/ready`
     says `ok`. Find out why the disk damaged data; a disk that keeps doing so is replaced
     (procedure 2).
   - A lost identity:

     ```
     identity:     LOST (NODE is missing or damaged)
     result:       the identity is lost; replace the member
     ```
     The node does not start (`error: /var/lib/freyja/NODE is missing or damaged; the node
     does not know who it is`). Move the directory aside for analysis
     (`mv /var/lib/freyja /var/lib/freyja.lost`) and replace the member as in procedure 2,
     with a new id.

## 4. A Full Disk and the Alarm for Lack of Space

**Signs.** The status says `alarms:          NOSPACE`, the metric `freyja_alarm_nospace` is 1,
the log says `the alarm for lack of space is set: only deletes are accepted`, and clients get
`NoSpace` for every write but deletes. The alarm is raised when the state machine of a node
holds more than its `--quota-bytes` (16 GiB by default) or its file system has less than
`--min-free-bytes` free (1 GiB by default). One node is enough; the alarm holds for the
whole cluster.

1. Find the node and the cause in the figures of each node:

   ```
   curl -s http://10.0.0.13:7400/metrics | grep -E "alarm_nospace|disk_free_bytes|node_stored_bytes"
   ```

2. Remove the cause on that node:
   - free space on its file system, or grow the file system;
   - delete keys through the clients (deletes are accepted), then let the nodes compact:
     `freyja-admin HOST:7300 compact` on each node;
   - shorten the Raft log of a node whose log takes the space: `freyja-admin HOST:7300 snapshot`;
   - or raise `--quota-bytes` or lower `--min-free-bytes` and restart the node.

3. Clear the alarm, at the leader:

   ```
   freyja-admin 10.0.0.10:7300 clear-alarm
   ```
   ```
   the clearing of the alarm is applied, at index I
   ```

4. Check a few seconds later that the status says `alarms:          none` and writes
   succeed. If the cause is still there, the node raises the alarm again within about a
   second, and the log says so again.

## 5. Rolling Upgrade, Finalization, and Downgrade

**Not executable yet:** it needs a second release. The transfer of leadership, the restart
of one node at a time, and raising the cluster version were tried with one release.
**Known gap:** the guard of the finalization (`docs/spec/operations.md` §6: refused unless
every member runs the new release) is not implemented. `set-version` takes any higher
version. Raise the cluster version only to a version that every member supports.

1. Before: every member is `ok`, a backup is taken (procedure 6), and the release notes say
   which cluster version the new release introduces.

2. One node at a time, the leader last:
   1. If the node leads, hand the leadership over, at the leader, and check that the other
      node leads now:

      ```
      freyja-admin 10.0.0.10:7300 transfer 1
      ```
      ```
      the hand-over to node 1 has begun
      ```
   2. Stop the node, replace the jars in `/opt/freyja/lib` with those of the new release,
      start the node.
   3. Wait until its `/ready` says `ok` and the leader reports it `ok`. The status of the node
      names its software: `node:            0 of cluster 1f, software X.Y.Z`.

3. Downgrade, as long as the cluster version has not been raised: the same steps with the
   jars of the old release.

4. Finalize when every node runs the new release, at the leader:

   ```
   freyja-admin 10.0.0.11:7300 set-version 2
   ```
   ```
   cluster version 2 is applied, at index I
   ```
   From then on the new release writes its new formats, and there is no way back: a lower
   version is refused (`refused: cluster version 1 was refused by the state machine: the
   cluster version is higher already`).

## 6. Backup, Verify, and Restore

**Backup.**

1. Ask any node for a backup into a directory on its machine that does not exist yet. The
   node is not held up while the backup is written:

   ```
   freyja-admin 10.0.0.13:7300 backup /var/backups/freyja/2026-11-19
   ```
   ```
   backup through index I in /var/backups/freyja/2026-11-19, 5 files
   ```

2. Verify it, copy it to where backups are kept, and verify the copy there:

   ```
   backup-verify /var/backups/freyja/2026-11-19
   ```
   ```
   backup of:    node 3 of cluster 1f, cluster version 1
   covers:       the log through index I of term 1, voters [0, 3, 4], learners []
   files:        5, 109437 bytes
   verdict:      the backup is whole
   ```
   Exit code 0. A backup that does not verify (`verdict:      the backup must not be relied
   on`, exit code 1) is not used.

**Restore.** A backup becomes a new cluster with a new cluster id, first with one node.

3. On the machine of the first node:

   ```
   freyja-node restore --backup /var/backups/freyja/2026-11-19 --data /var/lib/freyja --cluster 2f --node 0
   ```
   ```
   restored /var/lib/freyja as node 0, the only voter of the new cluster 2f, from a backup of cluster 1f through index I
   add further members as learners
   ```
   The backup is verified first; nothing is made of one that is not whole.

4. Start the node. It elects itself:

   ```
   node:            0 of cluster 2f, software 0.1.0
   role:            LEADER in term 3, leader is node 0
   log:             last I+2, committed I+2, applied I+2, starts after I
   ```

5. The addresses in the restored state are those of the old cluster. Set the addresses of
   the node:

   ```
   freyja-admin 10.0.0.20:7300 set-address 0 10.0.0.20:7100 10.0.0.20:7200
   ```

6. Add further members as in procedure 2, steps 2 to 6, with `--cluster 2f`. Point the
   clients at the new cluster.

## 7. Recover From a Lost Majority

**Only when a majority of the voters is lost for good.** Voters that are down but will come
back are waited for. This procedure loses what the lost nodes had acknowledged and the
survivor never received, and nobody can tell whether there was such a thing.

**Signs.** No node leads: `role:            PRE_CANDIDATE in term N, no leader known`,
`/ready` says `the node knows no leader`, every write runs out of time.

1. Of the surviving nodes, pick the one with the highest `last` index in its status. Stop
   every surviving node.

2. Verify the data directory of the chosen one; it must say `verdict:      nothing is
   damaged`. The recovery refuses a damaged log and a node that was refilling its log.

3. Copy its data directory aside (`cp -a /var/lib/freyja /var/lib/freyja.before-recovery`).

4. Make it the only voter of a new cluster:

   ```
   freyja-node recover-majority --data /var/lib/freyja --cluster 3f --i-accept-data-loss yes
   ```
   ```
   This starts a new cluster from the one node in /var/lib/freyja.
   It is NOT SAFE: whatever the lost nodes had acknowledged and this node never received
   is gone, and nobody can say whether there was such a thing.
   done: the node is the only voter of the new cluster 3f; its log ended at index I
   ```
   Without `--i-accept-data-loss yes` it prints the same warning, then `error: not done; say
   --i-accept-data-loss yes if this is understood`, and changes nothing.

5. Start the node. It leads the new cluster:

   ```
   node:            0 of cluster 3f, software 0.1.0
   role:            LEADER in term 3, leader is node 0
   member:          node 0 voter, peers at 10.0.0.10:7100, clients at 10.0.0.10:7200, log agrees through I, ok
   ```

6. Add members as in procedure 2, each with a new id and an empty data directory prepared
   with `--cluster 3f`. The other survivors belong to the old cluster: their directories are
   moved aside, not started. Point the clients at the new cluster.

## 8. Rotate Certificates

A node, the tool of an administrator, and a client look at their stores and the token file
when a connection is made, at most once a second, and load what has changed. Nothing is
restarted, and connections that exist go on. Files that cannot be used leave the last good
ones in force; the log says so. The figures `freyja_security_reloads` and
`freyja_security_reload_failures` count both.

**A new certificate of the same authority** (one expires):

1. Make the new key store (Appendix A) and put it in place of the old file, one node, one
   administrator, one client at a time, in any order.
2. Check: `freyja_security_reloads` has risen on the node, `freyja_security_reload_failures`
   has not, and `freyja-admin HOST:7300 status` works.

**A new authority:**

1. Add the certificate of the new authority to every trust store, of every node,
   administrator, and client, beside the old one:

   ```
   keytool -importcert -alias freyja-ca-b -file ca-b.pem -keystore /etc/freyja/trust.p12 \
     -storetype PKCS12 -storepass "$(head -1 /etc/freyja/password)" -noprompt
   ```

2. Replace every key store with one whose certificate the new authority issued.

3. Remove the old authority from every trust store:

   ```
   keytool -delete -alias freyja-ca-a -keystore /etc/freyja/trust.p12 \
     -storetype PKCS12 -storepass "$(head -1 /etc/freyja/password)"
   ```

4. Check: the status works with the new certificate of the administrator; a certificate of
   the old authority is refused (the node closes the connection without an answer); after a
   node restarts, its peers connect to it with their new certificates and the leader
   reports every member `ok`.

**A token of clients:** add the new token as a line of the token file of every node, move
the clients over, then remove the old line.

## Appendix A. Certificates With `keytool`

One authority and a required name per role (`docs/spec/operations.md` §5): nodes carry
`OU=freyja-peer`, administrators `OU=freyja-admin`, clients `OU=freyja-client`. A
certificate of a node is made out for every name or address it is dialed by (subject
alternative names). All stores are PKCS12 files with the password in the first line of
`/etc/freyja/password`.

```
PW="$(head -1 /etc/freyja/password)"
kt() { keytool "$@" -storetype PKCS12 -storepass "$PW"; }

# The authority, kept offline.
kt -genkeypair -alias ca -keyalg EC -groupname secp256r1 -dname "CN=freyja-ca-a" -ext bc:c \
   -validity 3650 -keystore ca.p12
keytool -exportcert -alias ca -keystore ca.p12 -storepass "$PW" -rfc -file ca.pem

# The trust store: the certificate of the authority and nothing else.
kt -importcert -alias freyja-ca-a -file ca.pem -keystore trust.p12 -noprompt

# issue NAME DNAME [SAN]: a key store with a key and a certificate of the authority.
issue() {
  kt -genkeypair -alias key -keyalg EC -groupname secp256r1 -dname "$2" -validity 825 -keystore "$1.p12"
  keytool -certreq -alias key -keystore "$1.p12" -storepass "$PW" -file "$1.csr"
  keytool -gencert -alias ca -keystore ca.p12 -storepass "$PW" -infile "$1.csr" -outfile "$1.pem" \
     -rfc -validity 825 ${3:+-ext san=$3}
  kt -importcert -alias ca -file ca.pem -keystore "$1.p12" -noprompt
  kt -importcert -alias key -file "$1.pem" -keystore "$1.p12" -noprompt
  kt -delete -alias ca -keystore "$1.p12"
  rm "$1.csr"
}
issue node-0 "CN=node-0,OU=freyja-peer" "ip:10.0.0.10,dns:node-0.example"
issue admin  "CN=admin,OU=freyja-admin"
issue client "CN=client,OU=freyja-client"
```

The tool of an administrator then talks to a node with `--tls-key admin.p12 --tls-trust
trust.p12 --tls-password-file /etc/freyja/password --tls-name OU=freyja-peer`. Without these
options it says `the node did not answer as a node in insecure mode does (...); if it runs
with TLS, --tls-key and --tls-trust are needed`.
