package io.github.sz.freyja.node.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.github.sz.freyja.common.admin.AdminCodec;
import io.github.sz.freyja.common.admin.AdminRequest;
import io.github.sz.freyja.common.admin.AdminResponse;
import io.github.sz.freyja.common.admin.AdminResponse.Member;

/**
 * The requests of an administrator against three running nodes (docs/spec/operations.md,
 * section 1): what the status says of the members, which changes of the membership a leader
 * refuses, and when it raises the cluster version (section 6).
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class AdminClusterTest {

    private TcpCluster cluster;

    @AfterEach
    void stop() throws IOException {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static AdminResponse ask(TcpCluster.Server server, AdminRequest request) {
        CompletableFuture<byte[]> answer = new CompletableFuture<>();
        assertTrue(server.runtime.adminRequest(AdminCodec.encode(7, request), answer::complete));
        try {
            AdminCodec.ResponseEnvelope envelope = AdminCodec.decodeResponse(answer.get(30, TimeUnit.SECONDS));
            assertEquals(7, envelope.requestId());
            return envelope.response();
        } catch (Exception e) {
            throw new AssertionError("no answer to " + request, e);
        }
    }

    private static AdminResponse.Status status(TcpCluster.Server server) {
        return assertInstanceOf(AdminResponse.Status.class, ask(server, new AdminRequest.Status()));
    }

    private TcpCluster.Server leader() {
        TcpCluster.Server[] found = {null};
        TcpCluster.await("a leader", () -> {
            for (TcpCluster.Server server : cluster.servers) {
                if (server.running() && status(server).role().equals("LEADER")) {
                    found[0] = server;
                    return true;
                }
            }
            return false;
        });
        return found[0];
    }

    @Test
    void theStatusOfTheLeaderSaysHowEveryMemberIs() throws IOException {
        cluster = new TcpCluster(60_000);
        TcpCluster.Server leader = leader();
        TcpCluster.await("the leader has heard from both followers", () -> status(leader).members().stream()
                .allMatch(m -> m.state().equals(Member.OK) && m.matchIndex() >= 1));
        AdminResponse.Status seen = status(leader);
        assertEquals(3, seen.members().size());
        assertTrue(seen.members().stream().allMatch(Member::voter));
        assertEquals(TcpCluster.CLUSTER, seen.clusterId());
        assertEquals(leader.id, seen.leaderId());

        // A follower does not know how the others are.
        TcpCluster.Server follower = cluster.servers.get((leader.id + 1) % 3);
        AdminResponse.Status other = status(follower);
        assertEquals("FOLLOWER", other.role());
        assertTrue(other.members().stream().allMatch(m -> m.state().equals(Member.UNKNOWN) && m.matchIndex() == -1));

        // A node that has gone quiet shows.
        follower.stop();
        TcpCluster.await("the leader notices the silence", () -> status(leader).members().stream()
                .anyMatch(m -> m.nodeId() == follower.id && m.state().equals(Member.SILENT)));
    }

    @Test
    void aChangeThatWouldLeaveNoMajorityOfVotersThatAreWellIsRefused() throws IOException {
        cluster = new TcpCluster(60_000, 2);
        TcpCluster.Server leader = leader();
        TcpCluster.Server quiet = cluster.servers.get((leader.id + 1) % 3);
        TcpCluster.Server well = cluster.servers.get((leader.id + 2) % 3);
        TcpCluster.await("the leader has heard from both followers", () -> status(leader).members().stream()
                .allMatch(m -> m.state().equals(Member.OK)));
        quiet.stop();
        TcpCluster.await("the leader notices the silence", () -> status(leader).members().stream()
                .anyMatch(m -> m.nodeId() == quiet.id && m.state().equals(Member.SILENT)));

        // Without the node that is well, the leader and a silent node would be left.
        AdminResponse.Refused refused = assertInstanceOf(AdminResponse.Refused.class,
                ask(leader, new AdminRequest.RemoveMember(well.id, false)));
        assertTrue(refused.reason().contains("only 1 are well"), refused.reason());
        assertEquals(3, status(leader).members().size(), "nothing was proposed");

        // Removing the silent node leaves two voters that are well.
        AdminResponse.Done done = assertInstanceOf(AdminResponse.Done.class,
                ask(leader, new AdminRequest.RemoveMember(quiet.id, false)));
        assertTrue(done.detail().contains("is in the log at index"), done.detail());
        TcpCluster.await("the removal has taken effect", () -> status(leader).members().size() == 2);

        // A follower sends the administrator to the leader.
        AdminResponse.Refused elsewhere = assertInstanceOf(AdminResponse.Refused.class,
                ask(well, new AdminRequest.ClearSpaceAlarm()));
        assertEquals(leader.id, elsewhere.leaderId());
        assertInstanceOf(AdminResponse.Done.class, ask(leader, new AdminRequest.ClearSpaceAlarm()));
        assertInstanceOf(AdminResponse.Done.class, ask(leader, new AdminRequest.Scrub()));
        assertInstanceOf(AdminResponse.Done.class, ask(leader, new AdminRequest.Snapshot()));
        assertInstanceOf(AdminResponse.Done.class, ask(leader, new AdminRequest.SetClusterVersion(2)));
        TcpCluster.await("the cluster version is raised", () -> status(well).clusterVersion() == 2);
        // A command says what it did: the version never goes down.
        AdminResponse.Refused down = assertInstanceOf(AdminResponse.Refused.class,
                ask(leader, new AdminRequest.SetClusterVersion(1)));
        assertTrue(down.reason().contains("higher already"), down.reason());
        assertTrue(assertInstanceOf(AdminResponse.Done.class, ask(leader, new AdminRequest.SetAddress(well.id,
                "127.0.0.1:1", "127.0.0.1:2"))).detail().contains("is applied"));
    }

    @Test
    void theClusterVersionIsRaisedOnlyToAVersionThatEveryMemberSupports() throws IOException {
        cluster = new TcpCluster(60_000, 2);
        TcpCluster.Server leader = leader();
        TcpCluster.Server old = cluster.servers.get((leader.id + 1) % 3);
        TcpCluster.Server other = cluster.servers.get((leader.id + 2) % 3);
        TcpCluster.await("the leader has heard from both followers", () -> status(leader).members().stream()
                .allMatch(m -> m.state().equals(Member.OK)));

        // Only the leader knows what every member supports.
        AdminResponse.Refused elsewhere = assertInstanceOf(AdminResponse.Refused.class,
                ask(other, new AdminRequest.SetClusterVersion(2)));
        assertTrue(elsewhere.reason().contains("not the leader"), elsewhere.reason());
        assertEquals(leader.id, elsewhere.leaderId());

        // No member runs a release that supports version 3, not even the leader.
        AdminResponse.Refused unknown = assertInstanceOf(AdminResponse.Refused.class,
                ask(leader, new AdminRequest.SetClusterVersion(3)));
        assertTrue(unknown.reason().contains("node " + leader.id + " supports cluster versions up to 2"),
                unknown.reason());

        // One member is started again with a release that knows version 1 only.
        cluster.clusterVersions[old.id] = 1;
        old.stop();
        old.restart();
        TcpCluster.await("the leader knows what the member runs now", () -> leader.transport.clusterVersionOf(old.id)
                == 1 && status(leader).members().stream().allMatch(m -> m.state().equals(Member.OK)));
        AdminResponse.Refused older = assertInstanceOf(AdminResponse.Refused.class,
                ask(leader, new AdminRequest.SetClusterVersion(2)));
        assertTrue(older.reason().contains("node " + old.id + " supports cluster versions up to 1"), older.reason());
        assertTrue(older.reason().contains("every member must run a release"), older.reason());

        // A member that is silent cannot say what it runs.
        cluster.clusterVersions[old.id] = 2;
        old.stop();
        TcpCluster.await("the leader notices the silence", () -> status(leader).members().stream()
                .anyMatch(m -> m.nodeId() == old.id && m.state().equals(Member.SILENT)));
        AdminResponse.Refused silent = assertInstanceOf(AdminResponse.Refused.class,
                ask(leader, new AdminRequest.SetClusterVersion(2)));
        assertTrue(silent.reason().contains("node " + old.id + " has not been heard from lately"), silent.reason());
        assertEquals(1, status(leader).clusterVersion(), "nothing was proposed");

        // Every member runs a release that supports version 2.
        old.restart();
        TcpCluster.await("the member is back with the new release", () -> leader.transport.clusterVersionOf(old.id)
                == 2 && status(leader).members().stream().allMatch(m -> m.state().equals(Member.OK)));
        AdminResponse.Done done = assertInstanceOf(AdminResponse.Done.class,
                ask(leader, new AdminRequest.SetClusterVersion(2)));
        assertTrue(done.detail().contains("is applied"), done.detail());
        TcpCluster.await("every member has applied it", () -> cluster.servers.stream()
                .allMatch(s -> status(s).clusterVersion() == 2));

        // There is no way back: a member started with the old release does not run any more.
        cluster.clusterVersions[old.id] = 1;
        old.stop();
        old.restart();
        TcpCluster.await("the member has ended", () -> old.runtime.fatal() != null);
        assertTrue(old.runtime.fatal().getMessage().contains("the cluster runs cluster version 2, and this"
                + " software supports cluster versions up to 1"), old.runtime.fatal().getMessage());
        assertTrue(old.runtime.notAlive().contains("ended for good"), old.runtime.notAlive());
    }
}
