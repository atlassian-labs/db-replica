package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.AuroraMultiReplicaConsistency;
import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.api.ReplicaConsistencyMock;
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
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuroraMultiReplicaConsistencyTest {
    @Mock
    private Connection supplierConnection;
    @Mock
    private SuppliedCache<Collection<Database>> discoveredReplicasCache;

    private AuroraMultiReplicaConsistency sut;

    private Collection<Connection> mockReplicaConnections(int count) {
        return mockReplicaConnections(count, false);
    }

    private Collection<Connection> mockReplicaConnections(int count, boolean mockBrokenConnections) {
        Collection<Connection> replicas = new LinkedList<>();
        Collection<Database> nodes = new LinkedList<>();

        ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider = (url) -> () -> {
            if (mockBrokenConnections) {
                throw new RuntimeException();
            }
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

    @Test
    void shouldNotOpenConnectionIfNotNeeded() {
        //given
        mockReplicaConnections(5, true);
        final Supplier<Connection> connectionSupplier = mock(Supplier.class);
        sut = AuroraMultiReplicaConsistency.builder()
            .replicaConsistency(new ReplicaConsistency() {
                @Override
                public void write(Connection main) {

                }

                @Override
                public boolean isConsistent(Supplier<Connection> replica) {
                    // Don't use the supplier
                    return true;
                }
            })
            .discoveredReplicasCache(discoveredReplicasCache)
            .build();


        //when
        final Throwable throwable = catchThrowable(() -> sut.isConsistent(connectionSupplier));

        //then
        assertThat(throwable).doesNotThrowAnyException();
    }
}
