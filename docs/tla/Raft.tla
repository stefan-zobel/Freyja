-------------------------------- MODULE Raft --------------------------------
(***************************************************************************)
(* Elections with PreVote and log replication of the Freyja Raft core,     *)
(* with asynchronous persistence (docs/spec/raft.md).                      *)
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
(* A log is a sequence of terms. Payloads do not matter to the protocol.   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Nodes,      \* the node ids, natural numbers
          MaxTerm,    \* bound for model checking
          MaxLog,     \* bound for model checking
          MaxPending, \* bound for model checking
          MaxMsgs     \* bound for model checking

Nil == 99             \* "no node"

VARIABLES st,         \* st[i]: the state of node i, a record
          msgs,       \* the messages sent so far; any of them may arrive, any number of times
          elected     \* history: the pairs <<term, node>> of all elections won

vars == <<st, msgs, elected>>

Quorum(S) == 2 * Cardinality(S) > Cardinality(Nodes)

Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y

LastTerm(log) == IF Len(log) = 0 THEN 0 ELSE log[Len(log)]
TermAt(log, k) == IF k = 0 THEN 0 ELSE log[k]

\* The length of the longest common prefix of two logs.
Agree(a, b) ==
    LET n == Min(Len(a), Len(b))
        diff == {k \in 1..n : a[k] # b[k]}
    IN IF diff = {} THEN n ELSE MinOf(diff) - 1

\* What must be durable before a promise leaves: term, vote, log, and through which index
\* the log is known to equal the log of the leader of term acc.t.
Snap(n) == [term |-> n.term, vote |-> n.vote, log |-> n.log, acc |-> n.acc]

InitNode ==
    [term |-> 0, role |-> "F", vote |-> Nil, log |-> <<>>, commit |-> 0,
     votes |-> {}, pvotes |-> {}, denied |-> {}, match |-> [k \in Nodes |-> 0],
     acc |-> [t |-> 0, i |-> 0], pend |-> <<>>,
     d |-> [term |-> 0, vote |-> Nil, log |-> <<>>, acc |-> [t |-> 0, i |-> 0]]]

Init ==
    /\ st = [i \in Nodes |-> InitNode]
    /\ msgs = {}
    /\ elected = {}

(***************************************************************************)
(* Helpers that turn a node state into the next one.                       *)
(***************************************************************************)

\* The step from s to n hands out a snapshot if it changed something persistent. A change of
\* acc alone belongs to the state handed out last, or is durable at once if nothing is pending.
Push(s, n) ==
    IF n.term # s.term \/ n.vote # s.vote \/ n.log # s.log
    THEN [n EXCEPT !.pend = Append(s.pend, Snap(n))]
    ELSE IF n.acc = s.acc THEN n
    ELSE IF s.pend = <<>> THEN [n EXCEPT !.d.acc = n.acc]
    ELSE [n EXCEPT !.pend[Len(s.pend)].acc = n.acc]

Follower(s, t) ==
    IF t > s.term
    THEN [s EXCEPT !.term = t, !.vote = Nil, !.role = "F", !.votes = {}, !.pvotes = {}, !.denied = {}]
    ELSE s

BecomeCandidate(s, i) ==
    [s EXCEPT !.term = s.term + 1, !.vote = i, !.role = "C", !.votes = {}, !.pvotes = {}, !.denied = {}]

\* A candidate counts its own vote only once that vote is durable.
MaybeLead(n, i) ==
    LET own == IF n.d.term = n.term /\ n.d.vote = i THEN {i} ELSE {}
    IN IF n.role = "C" /\ Quorum(n.votes \cup own)
       THEN [n EXCEPT !.role = "L", !.log = Append(n.log, n.term),
                      !.match = [k \in Nodes |-> 0]]
       ELSE n

\* A leader counts itself only up to what it has durably stored, and commits only through
\* an entry of its own term.
AdvanceCommit(n, i) ==
    LET m == [k \in Nodes |-> IF k = i THEN Agree(n.d.log, n.log) ELSE n.match[k]]
        ok == {x \in 1..Len(n.log) : Quorum({k \in Nodes : m[k] >= x})}
        best == IF ok = {} THEN 0 ELSE CHOOSE x \in ok : \A y \in ok : y <= x
    IN IF n.role = "L" /\ best > n.commit /\ n.log[best] = n.term
       THEN [n EXCEPT !.commit = best]
       ELSE n

Update(i, s, n) ==
    /\ st' = [st EXCEPT ![i] = Push(s, n)]
    /\ elected' = IF n.role = "L" /\ s.role # "L"
                  THEN elected \cup {<<n.term, i>>} ELSE elected

(***************************************************************************)
(* Messages. ValidSend(m, n) says whether the node with state n may send m.*)
(***************************************************************************)

UpToDate(lterm, lidx, log) ==
    \/ lterm > LastTerm(log)
    \/ lterm = LastTerm(log) /\ lidx >= Len(log)

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
                               /\ r.term = m.term /\ UpToDate(r.lterm, r.lidx, n.log)
      [] m.type = "RVR" /\ ~m.pre /\ m.granted -> \* a promise: the vote is durable
            n.d.term = m.term /\ n.d.vote = m.to
      [] m.type = "RVR" /\ ~m.granted -> m.term = n.term
      [] m.type = "AE" ->
            /\ n.role = "L" /\ m.term = n.term
            /\ m.prev + Len(m.ents) <= Len(n.log)
            /\ m.pterm = TermAt(n.log, m.prev)
            /\ m.ents = SubSeq(n.log, m.prev + 1, m.prev + Len(m.ents))
            /\ m.commit = n.commit
      [] m.type = "AER" /\ m.ok ->            \* a promise: the entries are durable
            n.d.acc.t = m.term /\ m.idx <= n.d.acc.i
      [] m.type = "AER" /\ ~m.ok -> m.term = n.term
      [] m.type = "HB" ->
            /\ n.role = "L" /\ m.term = n.term
            /\ m.commit = Min(n.match[m.to], n.commit)
      [] m.type = "HBR" -> m.term = n.term

(***************************************************************************)
(* Steps of a node that are not caused by a message.                       *)
(***************************************************************************)

\* The election timer fires: ask whether an election could be won. A node that is its own
\* majority becomes candidate at once.
Timeout(i) ==
    LET s == st[i]
        p == [s EXCEPT !.role = "P", !.votes = {}, !.pvotes = {}, !.denied = {}]
    IN /\ s.role # "L"
       /\ Update(i, s, IF Quorum({i}) THEN BecomeCandidate(p, i) ELSE p)

\* A leader that lost its majority gives up.
StepDown(i) ==
    LET s == st[i]
    IN /\ s.role # "F"
       /\ Update(i, s, [s EXCEPT !.role = "F", !.votes = {}, !.pvotes = {}, !.denied = {}])

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

Persist(i) == PersistK(i, 1)

\* A leader appends a command.
Propose(i) ==
    LET s == st[i]
    IN /\ s.role = "L"
       /\ Update(i, s, [s EXCEPT !.log = Append(s.log, s.term)])

\* A crash and restart. Of the snapshots on their way, any prefix may have become durable.
\* The state machine keeps any prefix of what it had applied that is still in the log.
RestartNode(s, k, c) ==
    LET dd == IF k = 0 THEN s.d ELSE s.pend[k]
    IN [InitNode EXCEPT !.term = dd.term, !.vote = dd.vote, !.log = dd.log, !.acc = dd.acc,
                        !.commit = c, !.d = dd]

Restart(i) ==
    LET s == st[i]
    IN \E k \in 0..Len(s.pend) :
         LET dd == IF k = 0 THEN s.d ELSE s.pend[k]
         IN \E c \in 0..Min(s.commit, Agree(dd.log, s.log)) :
              /\ st' = [st EXCEPT ![i] = RestartNode(s, k, c)]
              /\ UNCHANGED elected

(***************************************************************************)
(* Steps caused by a message m arriving at node m.to.                      *)
(***************************************************************************)

\* A request for a real vote. Granting is never required.
RecvRequestVote(m) ==
    LET i == m.to
        s == st[i]
        f == Follower(s, m.term)
    IN /\ m.type = "RV" /\ ~m.pre /\ m.term >= s.term
       /\ \/ Update(i, s, f)
          \/ /\ f.vote \in {Nil, m.from}
             /\ UpToDate(m.lterm, m.lidx, f.log)
             /\ Update(i, s, [f EXCEPT !.vote = m.from])

\* An answer to a request for a vote or a pre-vote. Of several answers of one node in one
\* round only the first counts. A node that a majority has turned down gives up.
RecvVoteResponse(m) ==
    LET i == m.to
        s == st[i]
        known == s.votes \cup s.pvotes \cup s.denied
        denied == s.denied \cup {m.from}
        turnedDown == IF Quorum(denied)
                      THEN [s EXCEPT !.role = "F", !.votes = {}, !.pvotes = {}, !.denied = {}]
                      ELSE [s EXCEPT !.denied = denied]
    IN /\ m.type = "RVR"
       /\ \/ /\ ~(m.pre /\ m.granted) /\ m.term > s.term
             /\ Update(i, s, Follower(s, m.term))
          \/ /\ m.pre /\ s.role = "P" /\ m.from \in known
             /\ (m.granted /\ m.term = s.term + 1) \/ (~m.granted /\ m.term = s.term)
             /\ Update(i, s, s)
          \/ /\ m.pre /\ m.granted /\ s.role = "P" /\ m.term = s.term + 1 /\ m.from \notin known
             /\ LET pv == s.pvotes \cup {m.from}
                IN Update(i, s, IF Quorum(pv \cup {i}) THEN BecomeCandidate(s, i)
                                ELSE [s EXCEPT !.pvotes = pv])
          \/ /\ m.pre /\ ~m.granted /\ s.role = "P" /\ m.term = s.term /\ m.from \notin known
             /\ Update(i, s, turnedDown)
          \/ /\ ~m.pre /\ s.role = "C" /\ m.term = s.term /\ m.from \in known
             /\ Update(i, s, s)
          \/ /\ ~m.pre /\ m.granted /\ s.role = "C" /\ m.term = s.term /\ m.from \notin known
             /\ Update(i, s, MaybeLead([s EXCEPT !.votes = s.votes \cup {m.from}], i))
          \/ /\ ~m.pre /\ ~m.granted /\ s.role = "C" /\ m.term = s.term /\ m.from \notin known
             /\ Update(i, s, turnedDown)

RecvAppendEntries(m) ==
    LET i == m.to
        s == st[i]
        f == [Follower(s, m.term) EXCEPT !.role = "F", !.votes = {}, !.pvotes = {}, !.denied = {}]
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
                                               /\ log[m.prev + k] # m.ents[k]}
                   news == {k \in 1..n : m.prev + k > Len(log)}
                   first == IF conflicts # {} THEN MinOf(conflicts)
                            ELSE IF news # {} THEN MinOf(news) ELSE n + 1
                   newLog == IF first = n + 1 THEN log
                             ELSE SubSeq(log, 1, m.prev + first - 1) \o SubSeq(m.ents, first, n)
                   matched == m.prev + n
               IN Update(i, s, [f EXCEPT !.log = newLog,
                                         !.commit = Max(f.commit, Min(m.commit, matched)),
                                         !.acc = accOf(matched)])

RecvAppendResponse(m) ==
    LET i == m.to
        s == st[i]
    IN /\ m.type = "AER"
       /\ \/ /\ m.ok /\ s.role = "L" /\ m.term = s.term
             /\ Update(i, s, AdvanceCommit(
                    [s EXCEPT !.match[m.from] = Max(s.match[m.from], m.idx)], i))
          \/ /\ m.term > s.term
             /\ Update(i, s, Follower(s, m.term))

RecvHeartbeat(m) ==
    LET i == m.to
        s == st[i]
        f == [Follower(s, m.term) EXCEPT !.role = "F", !.votes = {}, !.pvotes = {}, !.denied = {}]
    IN /\ m.type = "HB" /\ m.term >= s.term
       /\ Update(i, s, [f EXCEPT !.commit = Max(f.commit, Min(m.commit, Len(f.log)))])

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
    \/ RecvLaterTerm(m)

(***************************************************************************)
(* The complete next-state relation: a node may send every message that is *)
(* valid in its state. The messages are drawn from MsgUniverse, which the  *)
(* trace specification sets to the messages of a recorded run.             *)
(***************************************************************************)

CONSTANT MsgUniverse

Send(i) ==
    \E m \in MsgUniverse :
       /\ m.from = i /\ m \notin msgs
       /\ ValidSend(m, st[i])
       /\ msgs' = msgs \cup {m}
       /\ UNCHANGED <<st, elected>>

NodeStep(i) == Timeout(i) \/ StepDown(i) \/ Persist(i) \/ Propose(i) \/ Restart(i)

Next ==
    \/ (\E i \in Nodes : NodeStep(i)) /\ UNCHANGED msgs
    \/ (\E m \in msgs : Receive(m)) /\ UNCHANGED msgs
    \/ \E i \in Nodes : Send(i)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The next-state relation for model checking. It keeps the state space    *)
(* small without losing behaviors that matter for safety:                  *)
(*  - The pre-vote is replaced by Campaign: a node may become candidate at *)
(*    any time. The pre-vote only restricts when that happens.             *)
(*  - Only messages that change the state of their receiver beyond its     *)
(*    term are sent: requests for votes, granted votes, appends, and       *)
(*    acknowledgments. An append with no entries does what a heartbeat     *)
(*    does. A rejection only raises a term, which a request for a vote     *)
(*    does as well.                                                        *)
(***************************************************************************)

Campaign(i) ==
    LET s == st[i]
    IN /\ s.role # "L"
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
          commit |-> n.commit]
         : j \in others, q \in {x \in (0..Len(n.log)) \X (0..Len(n.log)) : x[1] <= x[2]}}
   \cup {[type |-> "AER", from |-> i, to |-> j, term |-> n.d.acc.t, ok |-> TRUE,
          idx |-> n.d.acc.i] : j \in others}

SendMC(i) ==
    \E m \in SendableMC(i) :
       /\ m \notin msgs
       /\ ValidSend(m, st[i])
       /\ msgs' = msgs \cup {m}
       /\ UNCHANGED <<st, elected>>

NextMC ==
    \/ (\E i \in Nodes : Campaign(i) \/ StepDown(i) \/ Persist(i) \/ Propose(i) \/ Restart(i))
       /\ UNCHANGED msgs
    \/ (\E m \in msgs : Receive(m)) /\ UNCHANGED msgs
    \/ \E i \in Nodes : SendMC(i)

SpecMC == Init /\ [][NextMC]_vars

\* The nodes are interchangeable; model checking uses that.
Symmetry == Permutations(Nodes)

(***************************************************************************)
(* Bounds for model checking.                                              *)
(***************************************************************************)

Bounded ==
    /\ \A i \in Nodes : /\ st[i].term <= MaxTerm
                        /\ Len(st[i].log) <= MaxLog
                        /\ Len(st[i].pend) <= MaxPending
    /\ Cardinality(msgs) <= MaxMsgs

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

ElectionSafety ==
    \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

\* Logs that hold the same term at an index are equal up to that index.
LogMatching ==
    \A i, j \in Nodes :
       \A k \in 1..Min(Len(st[i].log), Len(st[j].log)) :
          st[i].log[k] = st[j].log[k] => SubSeq(st[i].log, 1, k) = SubSeq(st[j].log, 1, k)

\* What two nodes consider committed is the same.
StateMachineSafety ==
    \A i, j \in Nodes :
       \A k \in 1..Min(st[i].commit, st[j].commit) : st[i].log[k] = st[j].log[k]

\* A committed entry is durable on a majority.
CommittedIsDurable ==
    \A i \in Nodes :
       \A k \in 1..st[i].commit :
          Quorum({q \in Nodes : Len(st[q].d.log) >= k /\ st[q].d.log[k] = st[i].log[k]})

\* The commit index of a node never exceeds its log, and a leader has voted for itself durably.
NodeSanity ==
    \A i \in Nodes :
       /\ st[i].commit <= Len(st[i].log)
       /\ st[i].role = "L" => st[i].d.term = st[i].term /\ st[i].d.vote = i
       /\ st[i].d.term <= st[i].term
=============================================================================
