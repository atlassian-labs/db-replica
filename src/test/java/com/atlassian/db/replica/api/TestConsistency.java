package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import static com.atlassian.db.replica.api.Queries.SELECT_FOR_UPDATE;
import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class TestConsistency {

    @Parameterized.Parameters
    public static Collection<Boolean> isCompatibleWithPreviousVersion() {
        return Arrays.asList(true, false);
    }

    private final boolean shouldUseCompatibleWithPreviousVersion;

    public TestConsistency(boolean shouldUseCompatibleWithPreviousVersion) {
        this.shouldUseCompatibleWithPreviousVersion = shouldUseCompatibleWithPreviousVersion;
    }

    @Test
    public void shouldRefreshCommitted() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.commit();

        verify(consistency).write(any());
    }

    private DualConnection.Builder dualConnectionBuilder(ConnectionProviderMock connectionProvider,
                                                         ReplicaConsistency consistency) {
        DualConnection.Builder builder = DualConnection.builder(connectionProvider, consistency);
        if (shouldUseCompatibleWithPreviousVersion) {
            builder.compatibleWithPreviousVersion();
        }
        return builder;
    }

    @Test
    public void shouldNotRefreshWhenNotCommitted() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldNotRefreshRolledBack() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();
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
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldRefreshAfterAutoCommittedQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();
        connection.setAutoCommit(true);

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency).write(any());
    }

    @Test
    public void shouldRefreshUpdatesByDefault() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency).write(any());
    }

    @Test
    public void shouldRefreshAfterFunctionCall() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

        //noinspection SqlDialectInspection,SqlNoDataSourceInspection
        connection.prepareStatement("SELECT doSomething(1234)").executeQuery();

        verify(consistency).write(any());
    }

    @Test
    public void shouldNotRefreshAfterSelectForUpdate() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

        connection.prepareStatement(SELECT_FOR_UPDATE).executeQuery();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldSetAutoCommitRefreshWhenInTransaction() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(true);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

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
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();

        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldNotRefreshAfterReadOnMain() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(true);
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(false);
        final Connection dualConnection = dualConnectionBuilder(provider, consistency).build();

        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(consistency, never()).write(any());
    }

    @Test
    public void shouldRefreshUpdateAfterQueryOnTheSameStatement() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final ReplicaConsistency consistency = mock(ReplicaConsistency.class);
        when(consistency.isConsistent(any())).thenReturn(false);
        final Connection connection = dualConnectionBuilder(connectionProvider, consistency).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);

        preparedStatement.executeQuery();
        preparedStatement.executeUpdate();

        verify(consistency).write(any());
    }
}
