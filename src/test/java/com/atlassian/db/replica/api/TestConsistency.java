package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import org.junit.Test;

import java.sql.SQLException;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestConsistency {
    private final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();

    @Test
    public void shouldRefreshCommitted() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.commit();

        verify(consistency).write(any());
    }

    @Test
    public void shouldNotRefreshWhenNotCommitted() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldNotRefreshRolledBack() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();
        connection.setAutoCommit(false);

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.rollback();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldNotRefreshWhenQueryReplica() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldRefreshAfterAutoCommittedQuery() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();
        connection.setAutoCommit(true);

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency).write(any());
    }

    @Test
    public void shouldRefreshUpdatesByDefault() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency).write(any());
    }

    @Test
    public void shouldRefreshAfterFunctionCall() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.prepareStatement("SELECT doSomething(1234)").executeQuery();

        verify(consistency).write(any());
    }

    @Test
    public void shouldSetAutoCommitRefreshWhenInTransaction() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.setAutoCommit(true);

        verify(consistency).write(any());
    }

    @Test
    public void shouldSetAutoCommitNotRefresh() throws SQLException {
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final DualConnection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency, never()).write(any());
    }
}
