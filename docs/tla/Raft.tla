-------------------------------- MODULE Raft --------------------------------
(***************************************************************************)
(* The Freyja Raft core (docs/spec/raft.md): elections with PreVote, log   *)
(* replication with asynchronous persistence, membership change with       *)
(* learners, leadership transfer, and the log-recovering mode.             *)
(*                                                                         *)
(* One action of this specification is one step of a node of the           *)
(* implementation. What the implementation decides by timers and flow      *)
(* control is left open here: a node may time out, step down, or send any  *)
(* message that is valid in its state, at any time.                        *)
(*                                                                         *)
(* A node has volatile state, durable state (field d), and a queue of      *)
(* snapshots handed out to be made durable (field pend). A message that    *)
(* promises something may be sent only if the durable state backs it.      *)
(* A restart loses everything but the durable state.                       *)
(*                                                                         *)
(* A log entry is a term and, for a CONFIG entry, the complete new         *)
(* configuration. Payloads do not matter to the protocol.                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Nodes,      \* the ids of all nodes that exist or may join
          InitVoters, \* the voters the cluster starts with
          MsgUniverse,\* the messages a node may send in the complete specification
          MaxTerm,    \* bounds for model checking
          MaxLog,
          MaxPending,
          MaxMsgs,
          MaxDamage

Nil == 99             \* "no node"

VARIABLES st,         \* st[i]: the state of node i, a record
          msgs,       \* the messages sent so far; any of them may arrive, any number of times
          elected,    \* history: the pairs <<term, node>> of all elections won
          chosen,     \* history: the entries seen as committed, with the term of the observer
          damages     \* history: the number of times a log was damaged

vars == <<st, msgs, elected, chosen, damages>>

Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

(***************************************************************************)
(* Logs and configurations.                                                *)
(***************************************************************************)

NoCfg == [v |-> {}, l |-> {}]
Entry(t) == [term |-> t, cfg |-> NoCfg]
InitCfg == [v |-> InitVoters, l |-> {}]

LastTerm(log) == IF Len(log) = 0 THEN 0 ELSE log[Len(log)].term
TermAt(log, k) == IF k = 0 THEN 0 ELSE log[k].term

\* The index of the entry that carries the configuration in force, 0 for the initial one.
CfgIndex(log) ==
    LET S == {k \in 1..Len(log) : log[k].cfg # NoCfg}
    IN IF S = {} THEN 0 ELSE MaxOf(S)

\* The configuration in force: the newest one in the log, committed or not.
CfgOf(log) == IF CfgIndex(log) = 0 THEN InitCfg ELSE log[CfgIndex(log)].cfg
Voters(n) == CfgOf(n.log).v

\* S holds a majority of the voters V.
QuorumOf(S, V) == 2 * Cardinality(S \cap V) > Cardinality(V)

\* The length of the longest common prefix of two logs.
Agree(a, b) ==
    LET n == Min(Len(a), Len(b))
        diff == {k \in 1..n : a[k] # b[k]}
    IN IF diff = {} THEN n ELSE MinOf(diff) - 1

\* The configurations one membership change away from c.
OneStep(c) ==
       {[v |-> c.v, l |-> c.l \cup {x}] : x \in Nodes \ (c.v \cup c.l)}       \* add a learner
  \cup {[v |-> c.v \cup {x}, l |-> c.l \ {x}] : x \in c.l}                   \* promote it
  \cup {[v |-> c.v \ {x}, l |-> c.l] : x \in IF Cardinality(c.v) > 1 THEN c.v ELSE {}}
  \cup {[v |-> c.v, l |-> c.l \ {x}] : x \in c.l}                            \* remove a learner

(***************************************************************************)
(* Node states.                                                            *)
(***************************************************************************)

\* What must be durable before a promise leaves: term, vote, log, through which index the
\* log is known to equal the log of the leader of term acc.t, and the log-recovering state.
Snap(n) == [term |-> n.term, vote |-> n.vote, log |-> n.log, acc |-> n.acc,
            rec |-> n.rec, epoch |-> n.epoch, lost |-> n.lost]

\* The end of a log, as a candidate states it and a voter compares it.
EndOf(log) == [i |-> Len(log), t |-> IF Len(log) = 0 THEN 0 ELSE log[Len(log)].term]
NoLoss == [i |-> 0, t |-> 0]

InitNode ==
    [term |-> 0, role |-> "F", vote |-> Nil, log |-> <<>>, commit |-> 0,
     votes |-> {}, pvotes |-> {}, denied |-> {},
     match |-> [k \in Nodes |-> 0],
     fep |-> [k \in Nodes |-> 0],  \* log epoch of each follower plus one; 0 for unknown
     acc |-> [t |-> 0, i |-> 0], rec |-> FALSE, epoch |-> 0,
     lost |-> NoLoss,              \* in log-recovering mode: the end of the log before the damage
     pend |-> <<>>,
     d |-> [term |-> 0, vote |-> Nil, log |-> <<>>, acc |-> [t |-> 0, i |-> 0],
            rec |-> FALSE, epoch |-> 0, lost |-> NoLoss]]

Init ==
    /\ st = [i \in Nodes |-> InitNode]
    /\ msgs = {}
    /\ elected = {}
    /\ chosen = {}
    /\ damages = 0

\* The step from s to n hands out a snapshot if it changed something persistent. A change of
\* acc alone belongs to the state handed out last, or is durable at once if nothing is pending.
Push(s, n) ==
    IF n.term # s.term \/ n.vote # s.vote \/ n.log # s.log \/ n.rec # s.rec \/ n.epoch # s.epoch
       \/ n.lost # s.lost
    THEN [n EXCEPT !.pend = Append(s.pend, Snap(n))]
    ELSE IF n.acc = s.acc THEN n
    ELSE IF s.pend = <<>> THEN [n EXCEPT !.d.acc = n.acc]
    ELSE [n EXCEPT !.pend[Len(s.pend)].acc = n.acc]

Idle(s) == [s EXCEPT !.role = "F", !.votes = {}, !.pvotes = {}, !.denied = {}]

Follower(s, t) ==
    IF t > s.term THEN [Idle(s) EXCEPT !.term = t, !.vote = Nil] ELSE s

BecomeCandidate(s, i) ==
    [Idle(s) EXCEPT !.term = s.term + 1, !.vote = i, !.role = "C"]

\* A candidate counts its own vote only once that vote is durable.
MaybeLead(n, i) ==
    LET own == IF n.d.term = n.term /\ n.d.vote = i THEN {i} ELSE {}
    IN IF n.role = "C" /\ QuorumOf(n.votes \cup own, Voters(n))
       THEN [n EXCEPT !.role = "L", !.log = Append(n.log, Entry(n.term)),
                      !.match = [k \in Nodes |-> 0], !.fep = [k \in Nodes |-> 0]]
       ELSE n

\* A leader counts itself only up to what it has durably stored, counts only voters, and
\* commits only through an entry of its own term. A leader that is no longer a voter steps
\* down once the change that removed it is committed.
AdvanceCommit(n, i) ==
    LET V == Voters(n)
        m == [k \in Nodes |-> IF k = i THEN Agree(n.d.log, n.log) ELSE n.match[k]]
        ok == {x \in 1..Len(n.log) : QuorumOf({k \in Nodes : m[k] >= x}, V)}
        best == IF ok = {} THEN 0 ELSE MaxOf(ok)
    IN IF n.role = "L" /\ best > n.commit /\ n.log[best].term = n.term
       THEN LET c == [n EXCEPT !.commit = best]
            IN IF i \notin V /\ best >= CfgIndex(n.log) THEN Idle(c) ELSE c
       ELSE n

Update(i, s, n) ==
    /\ st' = [st EXCEPT ![i] = Push(s, n)]
    /\ elected' = IF n.role = "L" /\ s.role # "L"
                  THEN elected \cup {<<n.term, i>>} ELSE elected
    /\ chosen' = chosen \cup {[idx |-> x, term |-> n.log[x].term, ct |-> n.term]
                              : x \in (s.commit + 1)..n.commit}
    /\ UNCHANGED damages

(***************************************************************************)
(* Messages. ValidSend(m, n) says whether the node with state n may send m.*)
(***************************************************************************)

\* A candidate whose log ends at (lterm, lidx) is at least as up to date as a log that ends
\* at e. A node in log-recovering mode compares with the end of the log it has lost: it votes
\* as it would have before the damage.
Covers(lterm, lidx, e) == lterm > e.t \/ (lterm = e.t /\ lidx >= e.i)
VoterEnd(n) == IF n.rec THEN n.lost ELSE EndOf(n.log)
UpToDate(lterm, lidx, log) == Covers(lterm, lidx, EndOf(log))

ValidSend(m, n) ==
    CASE m.type = "RV" /\ m.pre ->
            /\ n.role = "P" /\ m.term = n.term + 1
            /\ m.lidx = Len(n.log) /\ m.lterm = LastTerm(n.log)
      [] m.type = "RV" /\ ~m.pre ->          \* a promise: the candidacy is durable
            /\ n.d.term = m.term /\ n.d.vote = m.from
            /\ m.lidx = Len(n.d.log) /\ m.lterm = LastTerm(n.d.log)
      [] m.type = "RVR" /\ m.pre /\ m.granted ->
            /\ m.term > n.term
            /\ \E r \in msgs : /\ r.type = "RV" /\ r.pre /\ r.from = m.to /\ r.to = m.from
                               /\ r.term = m.term /\ Covers(r.lterm, r.lidx, VoterEnd(n))
      [] m.type = "RVR" /\ ~m.pre /\ m.granted -> \* a promise: the vote is durable
            n.d.term = m.term /\ n.d.vote = m.to
      [] m.type = "RVR" /\ ~m.granted -> m.term = n.term
      [] m.type = "AE" ->
            /\ n.role = "L" /\ m.term = n.term
            /\ m.prev + Len(m.ents) <= Len(n.log)
            /\ m.pterm = TermAt(n.log, m.prev)
            /\ m.ents = SubSeq(n.log, m.prev + 1, m.prev + Len(m.ents))
            /\ m.commit = n.commit
            /\ m.fep = n.fep[m.to]
      [] m.type = "AER" /\ m.ok ->            \* a promise: the entries are durable
            n.d.acc.t = m.term /\ m.idx <= n.d.acc.i /\ m.ep = n.d.epoch
      [] m.type = "AER" /\ ~m.ok -> m.term = n.term /\ m.ep = n.epoch
      [] m.type = "HB" ->
            /\ n.role = "L" /\ m.term = n.term
            /\ m.commit = Min(n.match[m.to], n.commit)
      [] m.type = "HBR" -> m.term = n.term
      [] m.type = "TN" -> n.role = "L" /\ m.term = n.term

(***************************************************************************)
(* Steps of a node that are not caused by a message.                       *)
(***************************************************************************)

\* The election timer fires: ask whether an election could be won. A node that is its own
\* majority becomes candidate at once. Only a voter with an intact log starts an election.
Timeout(i) ==
    LET s == st[i]
        p == [Idle(s) EXCEPT !.role = "P"]
    IN /\ s.role # "L" /\ i \in Voters(s) /\ ~s.rec
       /\ Update(i, s, IF QuorumOf({i}, Voters(s)) THEN BecomeCandidate(p, i) ELSE p)

\* A leader that lost its majority gives up.
StepDown(i) ==
    LET s == st[i]
    IN /\ s.role # "F"
       /\ Update(i, s, Idle(s))

\* The oldest k snapshots handed out become durable. A candidate may now count its own vote,
\* and a leader its own entries.
PersistNode(s, i, k) ==
    LET p == [s EXCEPT !.d = s.pend[k], !.pend = SubSeq(s.pend, k + 1, Len(s.pend))]
    IN Push(p, AdvanceCommit(MaybeLead(p, i), i))

PersistK(i, k) ==
    LET s == st[i]
        n == PersistNode(s, i, k)
    IN /\ k \in 1..Len(s.pend)
       /\ st' = [st EXCEPT ![i] = n]
       /\ elected' = IF n.role = "L" /\ s.role # "L"
                     THEN elected \cup {<<n.term, i>>} ELSE elected
       /\ chosen' = chosen \cup {[idx |-> x, term |-> n.log[x].term, ct |-> n.term]
                                 : x \in (s.commit + 1)..n.commit}
       /\ UNCHANGED damages

Persist(i) == PersistK(i, 1)

\* A leader appends a command.
Propose(i) ==
    LET s == st[i]
    IN /\ s.role = "L"
       /\ Update(i, s, [s EXCEPT !.log = Append(s.log, Entry(s.term))])

\* A leader appends a membership change: one node at a time, only after it has committed an
\* entry of its own term, and only while no earlier change is uncommitted.
ProposeCfg(i) ==
    LET s == st[i]
    IN /\ s.role = "L"
       /\ s.commit > 0 /\ s.log[s.commit].term = s.term
       /\ CfgIndex(s.log) <= s.commit
       /\ \E c \in OneStep(CfgOf(s.log)) :
            Update(i, s, AdvanceCommit(
                [s EXCEPT !.log = Append(s.log, [term |-> s.term, cfg |-> c])], i))

\* A crash and restart. Of the snapshots on their way, any prefix may have become durable.
\* The state machine keeps any prefix of what it had applied that is still in the log.
RestartNode(s, k, c) ==
    LET dd == IF k = 0 THEN s.d ELSE s.pend[k]
    IN [InitNode EXCEPT !.term = dd.term, !.vote = dd.vote, !.log = dd.log, !.acc = dd.acc,
                        !.rec = dd.rec, !.epoch = dd.epoch, !.lost = dd.lost, !.commit = c,
                        !.d = dd]

Restart(i) ==
    LET s == st[i]
    IN \E k \in 0..Len(s.pend) :
         LET dd == IF k = 0 THEN s.d ELSE s.pend[k]
         IN \E c \in 0..Min(s.commit, Agree(dd.log, s.log)) :
              /\ st' = [st EXCEPT ![i] = RestartNode(s, k, c)]
              /\ UNCHANGED <<elected, chosen, damages>>

(***************************************************************************)
(* Log damage: the stored log of node i cannot be read from index k on,    *)
(* also where the node has acknowledged it. The damage marks itself: it is *)
(* found again at every start, so the log-recovering state counts as       *)
(* durable from the moment of the damage. The protocol repairs a minority: *)
(* the damage is allowed only while a majority of every configuration in   *)
(* force somewhere holds an intact log without node i.                     *)
(***************************************************************************)

Cut(log, k) == SubSeq(log, 1, Min(Len(log), k - 1))
CutAcc(acc, k) == [acc EXCEPT !.i = Min(acc.i, k - 1)]

DamageNode(s, k) ==
    LET end == EndOf(s.log)
        \* A second damage keeps the later of the two ends.
        lost == IF s.rec /\ Covers(s.lost.t, s.lost.i, end) THEN s.lost ELSE end
        cutSnap(x) == [x EXCEPT !.log = Cut(x.log, k), !.acc = CutAcc(x.acc, k),
                                !.rec = TRUE, !.epoch = s.epoch + 1, !.lost = lost]
        n == [Idle(s) EXCEPT !.log = Cut(s.log, k), !.acc = CutAcc(s.acc, k),
                             !.commit = Min(s.commit, k - 1),
                             !.rec = TRUE, !.epoch = s.epoch + 1, !.lost = lost,
                             !.d = cutSnap(s.d),
                             !.pend = [x \in 1..Len(s.pend) |-> cutSnap(s.pend[x])]]
    IN [n EXCEPT !.pend = Append(n.pend, Snap(n))]

DamageK(i, k) ==
    LET s == st[i]
        intact == {j \in Nodes \ {i} : ~st[j].rec}
    IN /\ k \in 1..Len(s.log)
       /\ \A j \in Nodes : QuorumOf(intact, Voters(st[j]))
       /\ st' = [st EXCEPT ![i] = DamageNode(s, k)]
       /\ damages' = damages + 1
       /\ UNCHANGED <<elected, chosen>>

Damage(i) == \E k \in 1..Len(st[i].d.log) : DamageK(i, k)

(***************************************************************************)
(* Steps caused by a message m arriving at node m.to.                      *)
(***************************************************************************)

\* A request for a real vote. Granting is never required. A node in log-recovering mode
\* compares the candidate with the log it has lost: it may lack entries it has acknowledged.
RecvRequestVote(m) ==
    LET i == m.to
        s == st[i]
        f == Follower(s, m.term)
    IN /\ m.type = "RV" /\ ~m.pre /\ m.term >= s.term
       /\ \/ Update(i, s, f)
          \/ /\ f.vote \in {Nil, m.from}
             /\ Covers(m.lterm, m.lidx, VoterEnd(f))
             /\ Update(i, s, [f EXCEPT !.vote = m.from])

\* An answer to a request for a vote or a pre-vote. Of several answers of one node in one
\* round only the first counts. A node that a majority has turned down gives up.
RecvVoteResponse(m) ==
    LET i == m.to
        s == st[i]
        V == Voters(s)
        known == s.votes \cup s.pvotes \cup s.denied
        denied == s.denied \cup {m.from}
        turnedDown == IF QuorumOf(denied, V) THEN Idle(s) ELSE [s EXCEPT !.denied = denied]
    IN /\ m.type = "RVR"
       /\ \/ /\ ~(m.pre /\ m.granted) /\ m.term > s.term
             /\ Update(i, s, Follower(s, m.term))
          \/ /\ m.pre /\ s.role = "P" /\ m.from \in known
             /\ (m.granted /\ m.term = s.term + 1) \/ (~m.granted /\ m.term = s.term)
             /\ Update(i, s, s)
          \/ /\ m.pre /\ m.granted /\ s.role = "P" /\ m.term = s.term + 1 /\ m.from \notin known
             /\ LET pv == s.pvotes \cup {m.from}
                IN Update(i, s, IF QuorumOf(pv \cup ({i} \cap V), V) THEN BecomeCandidate(s, i)
                                ELSE [s EXCEPT !.pvotes = pv])
          \/ /\ m.pre /\ ~m.granted /\ s.role = "P" /\ m.term = s.term /\ m.from \notin known
             /\ Update(i, s, turnedDown)
          \/ /\ ~m.pre /\ s.role = "C" /\ m.term = s.term /\ m.from \in known
             /\ Update(i, s, s)
          \/ /\ ~m.pre /\ m.granted /\ s.role = "C" /\ m.term = s.term /\ m.from \notin known
             /\ Update(i, s, MaybeLead([s EXCEPT !.votes = s.votes \cup {m.from}], i))
          \/ /\ ~m.pre /\ ~m.granted /\ s.role = "C" /\ m.term = s.term /\ m.from \notin known
             /\ Update(i, s, turnedDown)

\* An append. A node in log-recovering mode leaves the mode if the append names its present
\* epoch, leaves its log equal to the log of the leader through the commit index of the
\* append, and that commit index is at an entry of the leader's own term.
RecvAppendEntries(m) ==
    LET i == m.to
        s == st[i]
        f == Idle(Follower(s, m.term))
        log == f.log
        n == Len(m.ents)
        accOf(x) == [t |-> m.term, i |-> IF f.acc.t = m.term THEN Max(f.acc.i, x) ELSE x]
    IN /\ m.type = "AE" /\ m.term >= s.term
       /\ IF m.prev < f.commit
          THEN \* everything up to the commit index equals the log of the leader
               Update(i, s, [f EXCEPT !.acc = accOf(f.commit)])
          ELSE IF m.prev > Len(log) \/ TermAt(log, m.prev) # m.pterm
          THEN Update(i, s, f)                                   \* rejected
          ELSE LET conflicts == {k \in 1..n : m.prev + k <= Len(log)
                                               /\ log[m.prev + k].term # m.ents[k].term}
                   news == {k \in 1..n : m.prev + k > Len(log)}
                   first == IF conflicts # {} THEN MinOf(conflicts)
                            ELSE IF news # {} THEN MinOf(news) ELSE n + 1
                   newLog == IF first = n + 1 THEN log
                             ELSE SubSeq(log, 1, m.prev + first - 1) \o SubSeq(m.ents, first, n)
                   matched == m.prev + n
                   repaired == /\ f.rec /\ m.fep = f.epoch + 1 /\ matched >= m.commit
                               /\ TermAt(newLog, m.commit) = m.term
               IN Update(i, s, [f EXCEPT !.log = newLog,
                                         !.commit = Max(f.commit, Min(m.commit, matched)),
                                         !.acc = accOf(matched),
                                         !.rec = f.rec /\ ~repaired,
                                         !.lost = IF repaired THEN NoLoss ELSE f.lost])

\* An answer to an append. An answer of an earlier log epoch than the leader has seen from
\* the follower says nothing. With a later one the leader forgets what the follower had.
RecvAppendResponse(m) ==
    LET i == m.to
        s == st[i]
        old == s.fep[m.from]
        lost == m.ep + 1 > old /\ old # 0
        base == [s EXCEPT !.fep[m.from] = m.ep + 1,
                          !.match[m.from] = IF lost THEN 0 ELSE @]
    IN /\ m.type = "AER"
       /\ \/ /\ m.term > s.term
             /\ Update(i, s, Follower(s, m.term))
          \/ /\ s.role = "L" /\ m.term = s.term /\ m.ep + 1 < old
             /\ Update(i, s, s)
          \/ /\ s.role = "L" /\ m.term = s.term /\ m.ep + 1 >= old
             /\ Update(i, s, IF m.ok
                             THEN AdvanceCommit([base EXCEPT !.match[m.from] = Max(@, m.idx)], i)
                             ELSE base)

RecvHeartbeat(m) ==
    LET i == m.to
        s == st[i]
        f == Idle(Follower(s, m.term))
    IN /\ m.type = "HB" /\ m.term >= s.term
       /\ Update(i, s, [f EXCEPT !.commit = Max(f.commit, Min(m.commit, Len(f.log)))])

\* Leadership transfer: the leader orders a voter to start an election at once, without a
\* pre-vote. The node may also ignore the order.
RecvTimeoutNow(m) ==
    LET i == m.to
        s == st[i]
        f == Follower(s, m.term)
    IN /\ m.type = "TN" /\ m.term >= s.term
       /\ \/ Update(i, s, f)
          \/ /\ f.role = "F" /\ i \in Voters(f) /\ ~f.rec
             /\ Update(i, s, BecomeCandidate(f, i))

\* Any other message of a later term makes its receiver a follower of that term.
RecvLaterTerm(m) ==
    LET i == m.to
        s == st[i]
    IN /\ m.type = "HBR" /\ m.term > s.term
       /\ Update(i, s, Follower(s, m.term))

Receive(m) ==
    \/ RecvRequestVote(m)
    \/ RecvVoteResponse(m)
    \/ RecvAppendEntries(m)
    \/ RecvAppendResponse(m)
    \/ RecvHeartbeat(m)
    \/ RecvTimeoutNow(m)
    \/ RecvLaterTerm(m)

(***************************************************************************)
(* The complete next-state relation: a node may send every message that is *)
(* valid in its state. The messages are drawn from MsgUniverse, which the  *)
(* trace specification sets to the messages of a recorded run.             *)
(***************************************************************************)

Send(i) ==
    \E m \in MsgUniverse :
       /\ m.from = i /\ m \notin msgs
       /\ ValidSend(m, st[i])
       /\ msgs' = msgs \cup {m}
       /\ UNCHANGED <<st, elected, chosen, damages>>

NodeStep(i) == \/ Timeout(i) \/ StepDown(i) \/ Persist(i) \/ Propose(i) \/ ProposeCfg(i)
               \/ Restart(i) \/ Damage(i)

Next ==
    \/ (\E i \in Nodes : NodeStep(i)) /\ UNCHANGED msgs
    \/ (\E m \in msgs : Receive(m)) /\ UNCHANGED msgs
    \/ \E i \in Nodes : Send(i)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The next-state relation for model checking. It keeps the state space    *)
(* small without losing behaviors that matter for safety:                  *)
(*  - The pre-vote and the leadership transfer are replaced by Campaign: a *)
(*    voter with an intact log may become candidate at any time. Both only *)
(*    restrict or trigger when that happens.                               *)
(*  - Only messages that change the state of their receiver beyond its     *)
(*    term are sent: requests for votes, granted votes, appends, and       *)
(*    acknowledgments. An append with no entries does what a heartbeat     *)
(*    does. A rejection only raises a term, which a request for a vote     *)
(*    does as well; the rejection of a node in log-recovering mode, which  *)
(*    tells the leader its new epoch, is sent as well.                     *)
(***************************************************************************)

Campaign(i) ==
    LET s == st[i]
    IN /\ s.role # "L" /\ i \in Voters(s) /\ ~s.rec
       /\ Update(i, s, BecomeCandidate(s, i))

SendableMC(i) ==
    LET n == st[i]
        others == Nodes \ {i}
    IN  {[type |-> "RV", from |-> i, to |-> j, term |-> n.d.term, lidx |-> Len(n.d.log),
          lterm |-> LastTerm(n.d.log), pre |-> FALSE] : j \in others}
   \cup {[type |-> "RVR", from |-> i, to |-> j, term |-> n.d.term, pre |-> FALSE,
          granted |-> TRUE] : j \in others}
   \cup {[type |-> "AE", from |-> i, to |-> j, term |-> n.term, prev |-> q[1],
          pterm |-> TermAt(n.log, q[1]), ents |-> SubSeq(n.log, q[1] + 1, q[2]),
          commit |-> n.commit, fep |-> n.fep[j]]
         : j \in others, q \in {x \in (0..Len(n.log)) \X (0..Len(n.log)) : x[1] <= x[2]}}
   \cup {[type |-> "AER", from |-> i, to |-> j, term |-> n.d.acc.t, ok |-> TRUE,
          idx |-> n.d.acc.i, ep |-> n.d.epoch] : j \in others}
   \cup {[type |-> "AER", from |-> i, to |-> j, term |-> n.term, ok |-> FALSE,
          idx |-> 0, ep |-> n.epoch] : j \in IF n.rec THEN others ELSE {}}

SendMC(i) ==
    \E m \in SendableMC(i) :
       /\ m \notin msgs
       /\ ValidSend(m, st[i])
       /\ msgs' = msgs \cup {m}
       /\ UNCHANGED <<st, elected, chosen, damages>>

NextMC ==
    \/ (\E i \in Nodes : \/ Campaign(i) \/ StepDown(i) \/ Persist(i) \/ Propose(i)
                         \/ ProposeCfg(i) \/ Restart(i) \/ Damage(i))
       /\ UNCHANGED msgs
    \/ (\E m \in msgs : Receive(m)) /\ UNCHANGED msgs
    \/ \E i \in Nodes : SendMC(i)

SpecMC == Init /\ [][NextMC]_vars

\* The nodes are interchangeable as far as the initial configuration treats them alike.
Symmetry == Permutations(InitVoters) \cup Permutations(Nodes \ InitVoters)

(***************************************************************************)
(* Bounds for model checking.                                              *)
(***************************************************************************)

Bounded ==
    /\ \A i \in Nodes : /\ st[i].term <= MaxTerm
                        /\ Len(st[i].log) <= MaxLog
                        /\ Len(st[i].pend) <= MaxPending
    /\ Cardinality(msgs) <= MaxMsgs
    /\ damages <= MaxDamage

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

ElectionSafety ==
    \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

\* Logs that hold an entry of the same term at an index are equal up to that index.
LogMatching ==
    \A i, j \in Nodes :
       \A k \in 1..Min(Len(st[i].log), Len(st[j].log)) :
          st[i].log[k].term = st[j].log[k].term
             => SubSeq(st[i].log, 1, k) = SubSeq(st[j].log, 1, k)

\* No two nodes ever see different entries as committed at the same index.
StateMachineSafety ==
    \A a, b \in chosen : a.idx = b.idx => a.term = b.term

\* An entry seen as committed is in the log of every leader of a later term.
LeaderCompleteness ==
    \A i \in Nodes :
       st[i].role = "L" =>
          \A c \in chosen :
             c.ct < st[i].term => Len(st[i].log) >= c.idx /\ st[i].log[c.idx].term = c.term

\* Without log damage and membership change, a committed entry is durable on a majority.
\* (An entry committed under an earlier configuration need not be on a majority of a later
\* one, and damage takes entries away; LeaderCompleteness covers those runs.)
CommittedIsDurable ==
    (damages = 0 /\ \A j \in Nodes : CfgIndex(st[j].log) = 0) =>
       \A i \in Nodes :
          \A k \in 1..st[i].commit :
             QuorumOf({q \in Nodes : Len(st[q].d.log) >= k /\ st[q].d.log[k] = st[i].log[k]},
                      InitVoters)

\* At most one configuration change is uncommitted in any log.
OneChangeAtATime ==
    LET top == IF chosen = {} THEN 0 ELSE MaxOf({c.idx : c \in chosen})
    IN \A i \in Nodes :
          Cardinality({k \in (top + 1)..Len(st[i].log) : st[i].log[k].cfg # NoCfg}) <= 1

\* The commit index of a node never exceeds its log, a leader has voted for itself durably,
\* and a node in log-recovering mode is no leader.
NodeSanity ==
    \A i \in Nodes :
       /\ st[i].commit <= Len(st[i].log)
       /\ st[i].role = "L" => st[i].d.term = st[i].term /\ st[i].d.vote = i /\ ~st[i].rec
       /\ st[i].d.term <= st[i].term
=============================================================================
