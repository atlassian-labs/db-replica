package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.aurora.AuroraReplicaNode;
import com.atlassian.db.replica.internal.util.ConnectionSupplier;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuroraMultiReplicaConsistencyTest {
    @Mock
    private Connection supplierConnection;

    private AuroraMultiReplicaConsistency sut;

    private Collection<Connection> mockReplicaConnections(
        int count, SuppliedCache<Collection<Database>> discoveredReplicasCache
    ) {
        return mockReplicaConnections(count, false, discoveredReplicasCache);
    }

    private Collection<Connection> mockReplicaConnections(
        int count, boolean mockBrokenConnections, SuppliedCache<Collection<Database>> discoveredReplicasCache
    ) {
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
        if (discoveredReplicasCache != null) {
            when(discoveredReplicasCache.get(any())).thenReturn(Optional.of(nodes));
        }

        return replicas;
    }

    @Test
    void shouldBeConsistentWhenNoReplicasAreAvailable() {
        //given
        final SuppliedCache discoveredReplicasCache = mock(SuppliedCache.class);
        mockReplicaConnections(0, discoveredReplicasCache);

        sut = AuroraMultiReplicaConsistency.builder().replicaConsistency(mock(ReplicaConsistency.class)).discoveredReplicasCache(
            discoveredReplicasCache).build();

        //when
        boolean consistent = sut.isConsistent(() -> supplierConnection);

        //then
        assertTrue(consistent);
    }

    @Test
    void shouldCloseReplicaConnectionsWhenConsistent() throws SQLException {
        //given
        final SuppliedCache discoveredReplicasCache = mock(SuppliedCache.class);
        Collection<Connection> replicas = mockReplicaConnections(5, discoveredReplicasCache);

        sut = AuroraMultiReplicaConsistency.builder().replicaConsistency(new ReplicaConsistencyMock(true)).discoveredReplicasCache(
            discoveredReplicasCache).build();

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
        final SuppliedCache discoveredReplicasCache = mock(SuppliedCache.class);
        Collection<Connection> replicas = mockReplicaConnections(5, discoveredReplicasCache);

        sut = AuroraMultiReplicaConsistency.builder().replicaConsistency(new ReplicaConsistencyMock(false)).discoveredReplicasCache(
            discoveredReplicasCache).build();

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
        final SuppliedCache discoveredReplicasCache = mock(SuppliedCache.class);
        mockReplicaConnections(5, true, discoveredReplicasCache);
        final Supplier<Connection> connectionSupplier = mock(Supplier.class);
        sut = AuroraMultiReplicaConsistency.builder().replicaConsistency(new ReplicaConsistency() {
            @Override
            public void write(Connection main) {

            }

            @Override
            public boolean isConsistent(Supplier<Connection> replica) {
                // Don't use the supplier
                return true;
            }
        }).discoveredReplicasCache(discoveredReplicasCache).build();


        //when
        final Throwable throwable = catchThrowable(() -> sut.isConsistent(connectionSupplier));

        //then
        assertThat(throwable).doesNotThrowAnyException();
    }

    @Test
    void shouldUseProvidedConnectionSupplier() throws SQLException {
        //given
        mockReplicaConnections(5, true, null);
        final Supplier<Connection> connectionSupplier = mock(Supplier.class);
        final String sql = "SELECT server_id, durable_lsn, current_read_lsn, feedback_xmin, " + "round(extract(milliseconds from (now()-last_update_timestamp))) as state_lag_in_msec, replica_lag_in_msec " + "FROM aurora_replica_status() " + "WHERE session_id != 'MASTER_SESSION_ID' and last_update_timestamp > NOW() - INTERVAL '5 minutes';";
        final ConnectionSupplier discovererConnectionSupplier = mock(ConnectionSupplier.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(discovererConnectionSupplier.get()).thenReturn(connection);
        when(connection.prepareStatement(eq(sql))).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(mock(ResultSet.class));
        sut = AuroraMultiReplicaConsistency.builder().replicaConsistency(new ReplicaConsistency() {
                @Override
                public void write(Connection main) {

                }

                @Override
                public boolean isConsistent(Supplier<Connection> replica) {
                    // Don't use the supplier
                    return true;
                }
            }).clusterUri(
                "jdbc:postgresql://database-1.cluster-crmnlihjxqlm.eu-central-1.rds.amazonaws.com:5432/asdf",
                discovererConnectionSupplier
            )
            .build();


        final Throwable throwable = catchThrowable(() -> sut.isConsistent(connectionSupplier));

        //then
        assertThat(throwable).doesNotThrowAnyException();
        verify(discovererConnectionSupplier).get();
    }
}
