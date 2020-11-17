package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import com.atlassian.db.replica.api.mocks.PermanentInconsistency;
import com.atlassian.db.replica.spi.DualConnectionOperation;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.atlassian.db.replica.api.Queries.LARGE_SQL_QUERY;
import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.MAIN;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.REPLICA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class TestDualConnection {
    private final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();

    @Test
    public void shouldUseReplicaConnectionForExecuteQuery() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldUseReplicaConnectionForExecuteLargeQuery() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(LARGE_SQL_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldUseMainConnectionForWrites() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate(SIMPLE_QUERY);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate(SIMPLE_QUERY, Statement.RETURN_GENERATED_KEYS);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate(SIMPLE_QUERY, new String[]{"test"});
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate(SIMPLE_QUERY, new int[]{123});
        connection.prepareStatement(SIMPLE_QUERY).executeLargeUpdate();
        connection.prepareStatement(SIMPLE_QUERY).executeLargeUpdate(SIMPLE_QUERY);
        connection.prepareStatement(SIMPLE_QUERY).executeLargeUpdate(SIMPLE_QUERY, Statement.RETURN_GENERATED_KEYS);
        connection.prepareStatement(SIMPLE_QUERY).executeLargeUpdate(SIMPLE_QUERY, new String[]{"test"});
        connection.prepareStatement(SIMPLE_QUERY).executeLargeUpdate(SIMPLE_QUERY, new int[]{123});
        connection.prepareStatement(SIMPLE_QUERY).executeLargeBatch();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .hasSize(1);
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForExecute() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).execute();
        connection.prepareStatement(SIMPLE_QUERY).execute(SIMPLE_QUERY);
        connection.prepareStatement(SIMPLE_QUERY).execute(SIMPLE_QUERY, Statement.RETURN_GENERATED_KEYS);
        connection.prepareStatement(SIMPLE_QUERY).execute(SIMPLE_QUERY, new String[]{"test"});
        connection.prepareStatement(SIMPLE_QUERY).execute(SIMPLE_QUERY, new int[]{123});
        connection.prepareStatement(SIMPLE_QUERY, new int[]{123}).execute(SIMPLE_QUERY, new int[]{123});

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .hasSize(1);
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldSwitchConnectionWhenPerformingWriteAfterRead() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
    }

    @Test
    public void shouldKeepUsingWriteConnectionAfterFirstWrite() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldNotUseReplicaIfNotAvailable() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final DualConnection connection = DualConnection.builder(provider, new PermanentInconsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(provider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldSwitchToMainIfNotConsistent() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
    }

    @Test
    public void shouldRunNativeSqlOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();

        connection.nativeSQL(SIMPLE_QUERY);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForSelectForUpdate() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement("select O_S_PROPERTY_ENTRY.id, O_S_PROPERTY_ENTRY.propertytype\n" +
            "from public.propertyentry O_S_PROPERTY_ENTRY\n" +
            "where O_S_PROPERTY_ENTRY.entity_name = ? and O_S_PROPERTY_ENTRY.entity_id = ? and O_S_PROPERTY_ENTRY.property_key = ?\n" +
            "order by O_S_PROPERTY_ENTRY.id desc\n" +
            "for update").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForSelectFunctionCalls() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement("SELECT concat_lower_or_upper('Hello', 'World', true)").executeQuery();
        connection.prepareStatement("select concat_lower_or_upper('Hello', 'World')").executeQuery();
        connection.prepareStatement("SELECT NEXTVAL('public.\"AO_21D670_WHITELIST_RULES_ID_seq\"')").executeQuery();
        connection.prepareStatement("SELECT doSomething()").executeQuery();
        connection.prepareStatement("SELECT doSomething(1234)").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldGetAutoCommitFalse() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.setAutoCommit(false);

        assertThat(connection.getAutoCommit()).isFalse();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldGetAutoCommitTrue() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.setAutoCommit(true);

        assertThat(connection.getAutoCommit()).isTrue();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldGetAutoCommitDefault() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        assertThat(connection.getAutoCommit()).isTrue();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldDualConnectionCloseSubConnections() throws SQLException {
        final DualConnection dualConnection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        dualConnection.close();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        connectionProvider.getProvidedConnections().forEach(connection -> {
            try {
                verify(connection).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldDualConnectionCloseSubConnectionsEvenIfMainConnectionCloseFails() throws SQLException {
        final DualConnection dualConnection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        doThrow(new RuntimeException("Connection already closed")).when(main).close();

        Throwable thrown = catchThrowable(dualConnection::close);

        assertThat(thrown).hasMessageContaining("Connection already closed");
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        connectionProvider.getProvidedConnections().forEach(connection -> {
            try {
                verify(connection).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldDualConnectionCloseSubConnectionsEvenIfReplicaConnectionCloseFails() throws SQLException {
        final DualConnection dualConnection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        doThrow(new RuntimeException("Connection already closed")).when(replica).close();
        Throwable thrown = catchThrowable(() -> dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate());

        dualConnection.close();

        assertThat(thrown).hasMessageContaining("Connection already closed");
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        connectionProvider.getProvidedConnections().forEach(connection -> {
            try {
                verify(connection).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldBeOpenAfterQueryExecution() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connection.isClosed()).isFalse();
    }

    @Test
    public void shouldBeClosedAfterCloseInvoked() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.close();

        assertThat(connection.isClosed()).isTrue();
    }

    @Test
    public void shouldInitiallyOpen() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        assertThat(connection.isClosed()).isFalse();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldNotCloseConnectionAfterExecuteQuery() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final DualConnection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();

        provider.getProvidedConnections().forEach(connection -> {
            try {
                Mockito.verify(connection, never()).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldCloseConnectionOnce() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final DualConnection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.close();

        provider.getProvidedConnections().forEach(connection -> {
            try {
                Mockito.verify(connection).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(dualConnection.isClosed()).isTrue();
    }

    @Test
    public void shouldSetAutocommit() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final DualConnection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

        dualConnection.setAutoCommit(false);
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.close();

        provider.getProvidedConnections().forEach(connection -> {
            try {
                Mockito.verify(connection).setAutoCommit(false);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(dualConnection.getAutoCommit()).isFalse();
    }

    @Test
    public void shouldCommitOnce() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final DualConnection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

        dualConnection.setAutoCommit(false);
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.commit();

        provider.getProvidedConnections().forEach(connection -> {
            try {
                Mockito.verify(connection).commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

    }

    @Test
    public void shouldRollbackOnce() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final DualConnection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

        dualConnection.setAutoCommit(false);
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.rollback();

        provider.getProvidedConnections().forEach(connection -> {
            try {
                Mockito.verify(connection).rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

    }

    @Test
    public void shouldHoldOnlyOneConnection() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        final Connection main = connectionProvider.getProvidedConnections().get(1);

        verify(replica).close();
        verify(main, never()).close();
    }

    @Test
    public void shouldExecuteOnReplica() throws SQLException {
        final DualConnectionOperation dualConnectionOperation = mock(DualConnectionOperation.class);
        final DualConnection connection = DualConnection
            .builder(connectionProvider, new PermanentConsistency())
            .dualConnectionOperation(dualConnectionOperation)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(dualConnectionOperation).executeOnReplica(any());
        verify(dualConnectionOperation, never()).executeOnMain(any());
    }

    @Test
    public void shouldExecuteOnMain() throws SQLException {
        final DualConnectionOperation dualConnectionOperation = mock(DualConnectionOperation.class);
        when(dualConnectionOperation.executeOnMain(any())).thenReturn(1);
        final DualConnection connection = DualConnection
            .builder(connectionProvider, new PermanentConsistency())
            .dualConnectionOperation(dualConnectionOperation)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(dualConnectionOperation, never()).executeOnReplica(any());
        verify(dualConnectionOperation).executeOnMain(any());
    }


    @Test
    public void shouldUsePrepareNewStatement() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        for (int i = 0; i < 10; i++) {
            connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        }

        assertThat(connectionProvider.getPreparedStatements())
            .hasSize(10);
    }

    @Test
    public void shouldGetMetaDataFromMaster() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.getMetaData();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).getMetaData();
    }

    @Test
    public void shouldStartReadOnlyMode() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.setReadOnly(true);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).setReadOnly(true);
        assertThat(connection.isReadOnly()).isTrue();
    }

    @Test
    public void shouldStopReadOnlyMode() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.setReadOnly(false);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).setReadOnly(false);
        assertThat(connection.isReadOnly()).isFalse();
    }
}
