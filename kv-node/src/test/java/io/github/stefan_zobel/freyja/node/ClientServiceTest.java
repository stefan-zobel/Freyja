package io.github.stefan_zobel.freyja.node;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.kv.ClientCodec;
import io.github.stefan_zobel.freyja.common.kv.ClientRequest;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.NotLeader;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Pairs;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Reason;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Rejected;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Result;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Value;
import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.Command.Condition;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Written;
import io.github.stefan_zobel.freyja.node.KvCluster.Member;

/**
 * The service for the requests of clients (docs/spec/node.md, sections 4.3, 7 and 8), with
 * complete nodes: requests go in as bytes and answers come out as bytes.
 */
class ClientServiceTest {

    private long requestId = 100;

    /** Sends a request to a node and returns where its answer will appear. */
    private List<ClientResponse> send(Member member, ClientRequest request, int timeoutMillis) throws IOException {
        List<ClientResponse> answers = new ArrayList<>();
        long id = ++requestId;
        member.service.request(ClientCodec.encode(id, timeoutMillis, request), frame -> {
            ClientCodec.ResponseEnvelope envelope = ClientCodec.decodeResponse(frame);
            assertEquals(id, envelope.requestId(), "the answer names its request");
            answers.add(envelope.response());
        });
        return answers;
    }

    /** Sends a request and lets the cluster work until it is answered. */
    private ClientResponse call(KvCluster cluster, Member member, ClientRequest request) throws IOException {
        List<ClientResponse> answers = send(member, request, 5_000);
        cluster.settle();
        for (int i = 0; i < 20 && answers.isEmpty(); i++) {
            cluster.ticks(1);
        }
        assertEquals(1, answers.size(), "one answer to " + request);
        return answers.getFirst();
    }

    private long register(KvCluster cluster, Member leader) throws IOException {
        Result result = assertInstanceOf(Result.class,
                call(cluster, leader, new ClientRequest.Execute(new Command.RegisterSession())));
        return assertInstanceOf(SessionRegistered.class, result.result()).sessionId();
    }

    private static ClientRequest put(long session, long sequence, String key, String value) {
        return new ClientRequest.Execute(new Command.Write(session, sequence,
                List.of(new Put(KvCluster.b(key), KvCluster.b(value)))));
    }

    @Test
    void commandsAndReadsAreServedByTheLeaderAndRedirectedByTheOthers() throws IOException {
        KvCluster cluster = KvCluster.withServices();
        Member leader = cluster.leader();
        Member follower = cluster.members.get((leader.id + 1) % 3);
        long session = register(cluster, leader);

        Result written = assertInstanceOf(Result.class, call(cluster, leader, put(session, 1, "b", "two")));
        assertEquals(new Written(), written.result());
        call(cluster, leader, put(session, 2, "a", "one"));
        call(cluster, leader, put(session, 3, "c", "three"));

        Value value = assertInstanceOf(Value.class, call(cluster, leader, new ClientRequest.Get(KvCluster.b("b"), false)));
        assertTrue(value.found());
        assertArrayEquals(KvCluster.b("two"), value.value());
        assertEquals(written.index(), value.modIndex());
        assertTrue(value.appliedIndex() >= written.index());
        Value missing = assertInstanceOf(Value.class,
                call(cluster, leader, new ClientRequest.Get(KvCluster.b("nothing"), false)));
        assertFalse(missing.found());

        Pairs pairs = assertInstanceOf(Pairs.class, call(cluster, leader,
                new ClientRequest.Scan(KvCluster.b("a"), KvCluster.b("c"), 10, false, false)));
        assertEquals(List.of("a", "b"), pairs.pairs().stream().map(p -> new String(p.key())).toList());
        Pairs all = assertInstanceOf(Pairs.class, call(cluster, leader,
                new ClientRequest.Scan(new byte[0], new byte[0], 2, false, false)));
        assertEquals(2, all.pairs().size(), "the limit of the request");

        // A follower sends the client to the leader, for commands and for linearizable reads.
        NotLeader redirect = assertInstanceOf(NotLeader.class, call(cluster, follower, put(session, 4, "d", "four")));
        assertEquals(leader.id, redirect.leaderId());
        assertEquals("node-" + leader.id + ":7000", redirect.leaderAddress());
        assertInstanceOf(NotLeader.class, call(cluster, follower, new ClientRequest.Get(KvCluster.b("b"), false)));
        // A stale read it answers itself, and says how far it has applied.
        Value stale = assertInstanceOf(Value.class, call(cluster, follower, new ClientRequest.Get(KvCluster.b("b"), true)));
        assertArrayEquals(KvCluster.b("two"), stale.value());
        assertTrue(stale.appliedIndex() > 0);

        for (Member member : cluster.members) {
            assertEquals(0, member.service.waitingCount());
        }
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aCommandThatIsSentAgainIsAnsweredFromTheRecordOfItsSession() throws IOException {
        KvCluster cluster = KvCluster.withServices();
        Member leader = cluster.leader();
        long session = register(cluster, leader);
        ClientRequest create = new ClientRequest.Execute(new Command.Conditional(session, 1,
                List.of(Condition.notExists(KvCluster.b("k"))), List.of(new Put(KvCluster.b("k"), KvCluster.b("v"))),
                List.of()));
        Result first = assertInstanceOf(Result.class, call(cluster, leader, create));
        assertEquals(new ConditionalApplied(true), first.result());
        Result again = assertInstanceOf(Result.class, call(cluster, leader, create));
        assertEquals(new ConditionalApplied(true), again.result(), "not looked at a second time");
        assertTrue(again.index() > first.index(), "it is in the log twice");
        Value value = assertInstanceOf(Value.class, call(cluster, leader, new ClientRequest.Get(KvCluster.b("k"), false)));
        assertEquals(first.index(), value.modIndex(), "written once");
        cluster.close();
    }

    @Test
    void whatAClientMayNotAskForIsRefused() throws IOException {
        KvCluster cluster = KvCluster.withServices();
        Member leader = cluster.leader();
        assertEquals(new Rejected(Reason.INVALID), call(cluster, leader,
                new ClientRequest.Execute(new Command.SetAlarm(Command.Alarm.NOSPACE))));
        assertEquals(new Rejected(Reason.INVALID), call(cluster, leader,
                new ClientRequest.Execute(new Command.SetClusterVersion(9))));
        assertEquals(new Rejected(Reason.UNSUPPORTED), call(cluster, leader,
                new ClientRequest.Scan(new byte[0], new byte[0], 5, true, false)));
        // Bytes that are no request get no answer.
        List<byte[]> answers = new ArrayList<>();
        leader.service.request(new byte[] {1, 2, 3}, answers::add);
        byte[] damaged = ClientCodec.encode(1, 100, new ClientRequest.Get(KvCluster.b("k"), false));
        damaged[5] ^= 1;
        leader.service.request(damaged, answers::add);
        cluster.settle();
        assertTrue(answers.isEmpty());
        assertEquals(2, leader.service.invalidCount());
        cluster.close();
    }

    @Test
    void aRequestThatCannotBeServedInItsTimeIsDroppedWithoutAnAnswer() throws IOException {
        KvCluster cluster = KvCluster.withServices();
        Member leader = cluster.leader();
        long session = register(cluster, leader);
        // The leader is cut off. It still believes that it leads, but nothing it is asked
        // for can be confirmed by a majority.
        cluster.isolated.add(leader.id);
        List<ClientResponse> write = send(leader, put(session, 1, "k", "v"), 300);
        List<ClientResponse> read = send(leader, new ClientRequest.Get(KvCluster.b("k"), false), 300);
        cluster.ticks(2);
        assertEquals(2, leader.service.waitingCount());
        assertTrue(write.isEmpty() && read.isEmpty());
        cluster.ticks(8);
        assertEquals(0, leader.service.waitingCount());
        assertTrue(write.isEmpty() && read.isEmpty(), "nobody waits for these answers any more");
        assertTrue(leader.service.droppedCount() >= 2);

        // A stale read is still answered: it needs nobody else.
        assertInstanceOf(Value.class, call(cluster, leader, new ClientRequest.Get(KvCluster.b("k"), true)));
        cluster.isolated.clear();
        cluster.ticks(30);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aLeaderThatIsDeposedWhileACommandWaitsSendsTheClientToTheNewLeader() throws IOException {
        KvCluster cluster = KvCluster.withServices();
        Member old = cluster.leader();
        long session = register(cluster, old);
        cluster.isolated.add(old.id);
        List<ClientResponse> answers = send(old, put(session, 1, "k", "old"), 60_000);
        cluster.ticks(40);
        Member next = cluster.leader();
        assertTrue(next != old);
        // The new leader takes the same command, with the same sequence number.
        assertEquals(new Written(), assertInstanceOf(Result.class, call(cluster, next, put(session, 1, "k", "old")))
                .result());
        assertTrue(answers.isEmpty(), "the old leader cannot know yet");

        cluster.isolated.clear();
        cluster.ticks(30);
        assertEquals(1, answers.size(), "its entry gave way to another: the command was lost there");
        NotLeader redirect = assertInstanceOf(NotLeader.class, answers.getFirst());
        assertEquals(next.id, redirect.leaderId());
        assertEquals(0, old.service.waitingCount());
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void whoeverWaitsWhenANodeStopsIsToldToAskAgain() throws IOException {
        KvCluster cluster = KvCluster.withServices();
        Member leader = cluster.leader();
        long session = register(cluster, leader);
        List<ClientResponse> answers = send(leader, put(session, 1, "k", "v"), 60_000);
        assertEquals(1, leader.service.waitingCount());
        leader.stop();
        assertEquals(List.of(new Rejected(Reason.UNAVAILABLE)), answers);
        // A request to a node that does not run is refused at once.
        assertEquals(List.of(new Rejected(Reason.UNAVAILABLE)), send(leader, put(session, 1, "k", "v"), 1_000));
        leader.start();
        cluster.ticks(40);
        Member now = cluster.leader();
        assertEquals(new Written(), assertInstanceOf(Result.class, call(cluster, now, put(session, 1, "k", "v")))
                .result());
        cluster.assertSameState();
        cluster.close();
    }
}
