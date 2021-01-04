package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestConsistency {

    @Test
    public void shouldRefreshCommitted() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.commit();

        verify(consistency).write(any());
    }

    @Test
    public void shouldNotRefreshWhenNotCommitted() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldNotRefreshRolledBack() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();
        connection.setAutoCommit(false);

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.rollback();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldNotRefreshWhenQueryReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldRefreshAfterAutoCommittedQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();
        connection.setAutoCommit(true);

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency).write(any());
    }

    @Test
    public void shouldRefreshUpdatesByDefault() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency).write(any());
    }

    @Test
    public void shouldRefreshAfterFunctionCall() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

        //noinspection SqlDialectInspection,SqlNoDataSourceInspection
        connection.prepareStatement("SELECT doSomething(1234)").executeQuery();

        verify(consistency).write(any());
    }

    @Test
    public void shouldSetAutoCommitRefreshWhenInTransaction() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.setAutoCommit(true);

        verify(consistency).write(any());
    }

    @Test
    public void shouldSetAutoCommitNotRefresh() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldNotRefreshAfterReadOnMain() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(true);
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(false);
        final Connection dualConnection = DualConnection.builder(
            provider,
            consistency
        ).build();

        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(consistency, never()).write(any());
    }

}
