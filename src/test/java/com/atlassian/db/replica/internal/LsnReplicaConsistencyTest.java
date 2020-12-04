package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.internal.util.ConnectionSupplier;
import com.atlassian.db.replica.spi.*;
import org.junit.*;

import java.sql.*;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class LsnReplicaConsistencyTest {

    @Test
    public void shouldBeConsistentIfReplicaGivesTheSameLsnAsMain() throws SQLException {
        final ReplicaConsistency consistency = new LsnReplicaConsistency();
        consistency.write(getConnection("16/3002D50"));

        final boolean isConsistent = consistency.isConsistent(getConnectionSupplier("16/3002D50"));

        assertThat(isConsistent).isTrue();
    }

    @Test
    public void shouldNotBeConsistentAfterFailedWrite() throws SQLException {
        final ReplicaConsistency consistency = new LsnReplicaConsistency();
        final Connection main = mock(Connection.class);
        when(main.prepareStatement(anyString())).thenThrow(new SQLException("Main connection fails"));

        consistency.write(main);

        final boolean isConsistent = consistency.isConsistent(getConnectionSupplier("16/3002D50"));

        assertThat(isConsistent).isFalse();
    }

    @Test
    public void shouldRecoverAfterFailedWrite() throws SQLException {
        final ReplicaConsistency consistency = new LsnReplicaConsistency();
        final Connection main = mock(Connection.class);
        when(main.prepareStatement(anyString())).thenThrow(new SQLException("Main connection fails"));

        consistency.write(main);
        consistency.write(getConnection("16/3002D50"));

        final boolean isConsistent = consistency.isConsistent(getConnectionSupplier("16/3002D50"));

        assertThat(isConsistent).isTrue();
    }

    @Test
    public void shouldNotBeConsistentBeforeTheFirstWrite() {
        final ReplicaConsistency consistency = new LsnReplicaConsistency();
        final Supplier<Connection> replica = mock(ConnectionSupplier.class);

        final boolean isConsistent = consistency.isConsistent(replica);

        assertThat(isConsistent).isFalse();
    }

    @Test
    public void shouldNotBeConsistentIfReplicaConnectionFails() throws SQLException {
        final ReplicaConsistency consistency = new LsnReplicaConsistency();
        final Connection mainConnection = getConnection("16/3002D50");
        consistency.write(mainConnection);
        final Connection replica = mock(Connection.class);
        when(replica.prepareStatement(anyString())).thenThrow(new SQLException("Replica connection fails"));

        final boolean isConsistent = consistency.isConsistent(new ConnectionSupplier(replica));

        assertThat(isConsistent).isFalse();
    }

    private Supplier<Connection> getConnectionSupplier(String lsn) throws SQLException {
        return new ConnectionSupplier(getConnection(lsn));
    }

    private Connection getConnection(String lsn) throws SQLException {
        final Connection main = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(main.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getString(anyString())).thenReturn(lsn);
        return main;
    }

}
