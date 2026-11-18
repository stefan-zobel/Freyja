------------------------------ MODULE RaftTrace ------------------------------
(***************************************************************************)
(* Trace validation (docs/spec/verification.md, section 2.7): constrains   *)
(* the specification Raft to one recorded run of the implementation.       *)
(*                                                                         *)
(* TraceData is generated from the run. Trace[p] is one step of one node:  *)
(* the event, the protocol state of the node after the step, and the       *)
(* messages the step released. A step is accepted if                       *)
(*  - an action of Raft that belongs to the event (see README.md) leads to *)
(*    a state of the node that equals the recorded one,                    *)
(*  - a received message was released before, and                          *)
(*  - every released message is one the node may send in its new state.    *)
(*                                                                         *)
(* The run is accepted if TLC gets past its last step, which shows as a    *)
(* violation of the "invariant" NotDone. A violation of any other          *)
(* invariant, or the end of the search before the last step, rejects it.   *)
(***************************************************************************)
EXTENDS Raft, TraceData

VARIABLE pos        \* the step of the trace to take next

tvars == <<st, msgs, elected, chosen, damages, pos>>

Stutter == UNCHANGED <<st, elected, chosen, damages>>

\* The recorded state of the node after the step.
Matches(e) ==
    LET n == st'[e.node]
    IN /\ n.term = e.term
       /\ n.role = e.role
       /\ n.vote = e.vote
       /\ Len(n.log) = e.lidx
       /\ LastTerm(n.log) = e.lterm
       /\ n.commit = e.commit
       /\ Agree(n.d.log, n.log) = e.durable
       /\ n.rec = e.rec
       /\ n.epoch = e.epoch
       \* A node that joins knows of no configuration before the first CONFIG entry reaches it.
       /\ IF CfgIndex(n.log) = 0 /\ e.node \notin InitVoters
          THEN e.voters = {} /\ e.learners = {}
          ELSE CfgOf(n.log) = [v |-> e.voters, l |-> e.learners]

\* A promise that waited for several snapshots is backed by one of them.
ValidRelease(m, e) ==
    \/ ValidSend(m, st'[e.node])
    \/ /\ e.ev = "Persist"
       /\ \E t \in 1..e.k : ValidSend(m, PersistNode(st[e.node], e.node, t))

\* Whether the implementation acts on message m in the current state of its receiver. If it
\* does, the step must be the action for m: a step that only changes what the trace does not
\* show (the votes counted, the match index of a follower) must not pass as "nothing happened".
\* A request for a vote is left out: it may be ignored (a leader was heard from recently), and
\* whether it was shows in the term and the vote.
ActsOn(m) ==
    LET s == st[m.to]
    IN CASE m.type = "RVR" -> \/ m.pre /\ m.granted /\ s.role = "P" /\ m.term = s.term + 1
                              \/ m.pre /\ ~m.granted /\ s.role = "P" /\ m.term = s.term
                              \/ ~m.pre /\ s.role = "C" /\ m.term = s.term
                              \/ ~(m.pre /\ m.granted) /\ m.term > s.term
         [] m.type = "AE"  -> m.term >= s.term
         [] m.type = "HB"  -> m.term >= s.term
         [] m.type = "AER" -> (s.role = "L" /\ m.term = s.term) \/ m.term > s.term
         [] m.type = "HBR" -> m.term > s.term
         [] OTHER -> FALSE

StepOf(e) ==
    LET i == e.node
    IN CASE e.ev = "Tick" ->
              \* A node that starts a pre-vote asks the others; a single node has no one to ask.
              IF \E m \in e.rel : m.type = "RV" /\ m.pre
              THEN Timeout(i)
              ELSE StepDown(i) \/ Stutter \/ (QuorumOf({i}, Voters(st[i])) /\ Timeout(i))
         [] e.ev = "Recv" ->
              /\ e.msg \in msgs
              /\ e.msg.to = i
              /\ IF ActsOn(e.msg) THEN Receive(e.msg)
                 ELSE IF (e.msg.type = "RV" /\ ~e.msg.pre) \/ e.msg.type = "TN"
                 THEN Receive(e.msg) \/ Stutter
                 ELSE Stutter
         [] e.ev = "Persist" -> IF e.k = 0 THEN Stutter ELSE PersistK(i, e.k)
         [] e.ev = "Propose" -> Propose(i) \/ Stutter
         [] e.ev = "Read"    -> Stutter
         [] e.ev = "Config"  -> ProposeCfg(i) \/ Stutter
         [] e.ev = "Transfer" -> Stutter
         \* Snapshots are not in the model. A run with snapshots cannot be validated.
         \* Damage beyond the log changes nothing. Whether a majority is intact is the
         \* business of whoever injects the damage, not of the node.
         [] e.ev = "Damage"  -> IF e.k > Len(st[i].log) THEN Stutter
                                ELSE /\ st' = [st EXCEPT ![i] = DamageNode(st[i], e.k)]
                                     /\ damages' = damages + 1
                                     /\ UNCHANGED <<elected, chosen>>
         [] e.ev = "Restart" -> Restart(i)

TraceInit == Init /\ pos = 1

\* The new values of all variables are fixed before the released messages are examined, so
\* that TLC evaluates their validity as a plain condition instead of trying every way in
\* which it can hold.
TraceNext ==
    /\ pos <= Len(Trace)
    /\ StepOf(Trace[pos])
    /\ msgs' = msgs \cup Trace[pos].rel
    /\ pos' = pos + 1
    /\ Matches(Trace[pos])
    /\ \A m \in Trace[pos].rel : m.from = Trace[pos].node /\ ValidRelease(m, Trace[pos])

TraceSpec == TraceInit /\ [][TraceNext]_tvars

NotDone == pos <= Len(Trace)
=============================================================================
