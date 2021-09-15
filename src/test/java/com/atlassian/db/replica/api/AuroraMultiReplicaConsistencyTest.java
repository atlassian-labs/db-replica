package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.aurora.AuroraReplicaNode;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuroraMultiReplicaConsistencyTest {
    @Mock
    private Connection supplierConnection;
    @Mock
    private SuppliedCache<Collection<Database>> discoveredReplicasCache;

    private AuroraMultiReplicaConsistency sut;

    private Collection<Connection> mockReplicaConnections(int count) {
        Collection<Connection> replicas = new LinkedList<>();
        Collection<Database> nodes = new LinkedList<>();

        ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider = (url) -> () -> {
            Connection replica = mock(Connection.class);
            replicas.add(replica);
            return replica;
        };

        for (int i = 0; i < count; i++) {
            AuroraReplicaNode node = new AuroraReplicaNode(
                String.valueOf(i),
                replicaConnectionPerUrlProvider.getReplicaConnectionProvider(null)
            );
            nodes.add(node);
        }
        when(discoveredReplicasCache.get(any())).thenReturn(Optional.of(nodes));

        return replicas;
    }

    @Test
    void shouldBeConsistentWhenNoReplicasAreAvailable() {
        //given
        mockReplicaConnections(0);

        sut = AuroraMultiReplicaConsistency.builder()
            .replicaConsistency(mock(ReplicaConsistency.class))
            .discoveredReplicasCache(discoveredReplicasCache)
            .build();

        //when
        boolean consistent = sut.isConsistent(() -> supplierConnection);

        //then
        assertTrue(consistent);
    }

    @Test
    void shouldCloseReplicaConnectionsWhenConsistent() throws SQLException {
        //given
        Collection<Connection> replicas = mockReplicaConnections(5);

        sut = AuroraMultiReplicaConsistency.builder()
            .replicaConsistency(new ReplicaConsistencyMock(true))
            .discoveredReplicasCache(discoveredReplicasCache)
            .build();

        //when
        sut.isConsistent(() -> supplierConnection);

        //then
        verify(supplierConnection, never()).close();
        for (Connection replica : replicas) {
            verify(replica).close();
        }
    }

    @Test
    void shouldCloseReplicaConnectionsWhenNotConsistent() throws SQLException {
        //given
        Collection<Connection> replicas = mockReplicaConnections(5);

        sut = AuroraMultiReplicaConsistency.builder()
            .replicaConsistency(new ReplicaConsistencyMock(false))
            .discoveredReplicasCache(discoveredReplicasCache)
            .build();

        //when
        sut.isConsistent(() -> supplierConnection);

        //then
        verify(supplierConnection, never()).close();
        for (Connection replica : replicas) {
            verify(replica).close();
        }
    }
}
