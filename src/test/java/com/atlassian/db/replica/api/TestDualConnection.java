package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionMock;
import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.NoOpConnection;
import com.atlassian.db.replica.api.mocks.NoOpConnectionProvider;
import com.atlassian.db.replica.api.mocks.ReadOnlyAwareConnection;
import com.atlassian.db.replica.api.mocks.SingleConnectionProvider;
import com.atlassian.db.replica.spi.DualCall;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.atlassian.db.replica.api.Queries.LARGE_SQL_QUERY;
import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentConsistency;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentInconsistency;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.MAIN;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.REPLICA;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class TestDualConnection {

    @Test
    public void shouldUseReplicaConnectionForExecuteQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldUseReplicaConnectionForExecuteLargeQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(LARGE_SQL_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldUseMainConnectionForWrites() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

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
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

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
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
    }

    @Test
    public void shouldKeepUsingWriteConnectionAfterFirstWrite() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldNotUseReplicaIfNotAvailable() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final Connection connection = DualConnection.builder(provider, permanentInconsistency().build()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(provider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldSwitchToMainIfNotConsistent() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
    }

    @Test
    public void shouldAvoidFetchingReadConnectionWhenNotNecessary() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().ignoreSupplier(true).build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldRunNativeSqlOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();

        connection.nativeSQL(SIMPLE_QUERY);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForSelectForUpdate() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

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
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement("SELECT concat_lower_or_upper('Hello', 'World', true)").executeQuery();
        connection.prepareStatement("select concat_lower_or_upper('Hello', 'World')").executeQuery();
        connection.prepareStatement("SELECT NEXTVAL('public.\"AO_21D670_WHITELIST_RULES_ID_seq\"')").executeQuery();
        connection.prepareStatement("SELECT doSomething()").executeQuery();
        connection.prepareStatement("SELECT doSomething(1234)").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseReplicaForComplexQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(
            "select \"project_id_1\" from ((select \"PROJECT_3\".\"id\" as \"project_id_1\" from \"public\".\"project\" \"PROJECT_3\" join \"public\".\"schemepermissions\" \"SCHEME_PERMISSIONS_4\" on \"PROJECT_3\".\"permissionscheme\" = \"SCHEME_PERMISSIONS_4\".\"scheme\" join \"public\".\"projectroleactor\" \"PROJECT_ROLE_ACTOR_5\" on \"SCHEME_PERMISSIONS_4\".\"perm_parameter\" = cast(\"PROJECT_ROLE_ACTOR_5\".\"projectroleid\" as varchar) and \"PROJECT_3\".\"id\" = \"PROJECT_ROLE_ACTOR_5\".\"pid\" where \"SCHEME_PERMISSIONS_4\".\"permission_key\" = $1").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldUseMainConnectionForUpdateInExecuteQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement("update \"jiraissue\" \"ISSUE\"\n" +
            "set \"version\" = \"ISSUE\".\"version\" + 1\n" +
            "where \"ISSUE\".\"id\" in (42, 43) returning \"ISSUE\".\"id\", \"ISSUE\".\"version\"").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForUpdateInExecuteQueryUpperCase() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement("UPDATE \"jiraissue\" \"ISSUE\"\n" +
            "set \"version\" = \"ISSUE\".\"version\" + 1\n" +
            "where \"ISSUE\".\"id\" in (42, 43) RETURNING \"ISSUE\".\"id\", \"ISSUE\".\"version\"").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForDeleteInExecuteQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement("delete from \"jiraissue\" \"ISSUE\"\n" +
            "where \"ISSUE\".\"id\" in (42, 43) returning \"ISSUE\".\"id\", \"ISSUE\".\"version\"").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForDeleteInExecuteQueryUpperCase() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement("DELETE FROM \"jiraissue\" \"ISSUE\"\n" +
            "where \"ISSUE\".\"id\" in (42, 43) RETURNING \"ISSUE\".\"id\", \"ISSUE\".\"version\"").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldGetAutoCommitFalse() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.setAutoCommit(false);

        assertThat(connection.getAutoCommit()).isFalse();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldGetAutoCommitTrue() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.setAutoCommit(true);

        assertThat(connection.getAutoCommit()).isTrue();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldGetAutoCommitDefault() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        assertThat(connection.getAutoCommit()).isTrue();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldDualConnectionCloseSubConnections() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
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
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        doThrow(new RuntimeException("Connection already closed")).when(main).close();

        Throwable thrown = catchThrowable(dualConnection::close);

        assertThat(thrown.getCause()).hasMessageContaining("Connection already closed");
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
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        doThrow(new RuntimeException("Connection already closed")).when(replica).close();
        Throwable thrown = catchThrowable(() -> dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate());

        dualConnection.close();

        assertThat(thrown.getCause()).hasMessageContaining("Connection already closed");
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
    public void shouldCloseSubConnectionsRegardlessOfWarnings() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        //noinspection ThrowableNotThrown
        doThrow(new RuntimeException("Connection already closed")).when(main).getWarnings();

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
    public void shouldBeOpenAfterQueryExecution() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connection.isClosed()).isFalse();
    }

    @Test
    public void shouldBeClosedAfterCloseInvoked() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.close();

        assertThat(connection.isClosed()).isTrue();
    }

    @Test
    public void shouldInitiallyOpen() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        assertThat(connection.isClosed()).isFalse();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldNotCloseConnectionAfterExecuteQuery() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final Connection dualConnection = DualConnection.builder(provider, permanentInconsistency().build()).build();

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
        final Connection dualConnection = DualConnection.builder(provider, permanentInconsistency().build()).build();

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
        final Connection dualConnection = DualConnection.builder(provider, permanentInconsistency().build()).build();

        dualConnection.setAutoCommit(false);
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();

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
        final Connection dualConnection = DualConnection.builder(provider, permanentInconsistency().build()).build();

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
        final Connection dualConnection = DualConnection.builder(provider, permanentInconsistency().build()).build();

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
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        final Connection main = connectionProvider.getProvidedConnections().get(1);

        verify(replica).close();
        verify(main, never()).close();
    }

    @Test
    public void shouldExecuteOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DualCall dualCall = mock(DualCall.class);
        final Connection connection = DualConnection
            .builder(connectionProvider, permanentConsistency().build())
            .dualCall(dualCall)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(dualCall).callReplica(any());
        verify(dualCall, never()).callMain(any());
    }

    @Test
    public void shouldExecuteOnMainWhenNotConsistent() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DualCall dualCall = mock(DualCall.class);
        final ReplicaConsistency consistency = permanentInconsistency().build();
        final Connection connection = DualConnection
            .builder(connectionProvider, consistency)
            .dualCall(dualCall)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA, MAIN);
        verify(dualCall).callMain(any());
        verify(dualCall, never()).callReplica(any());
    }

    @Test
    public void shouldExecuteOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DualCall dualCall = mock(DualCall.class);
        when(dualCall.callMain(any())).thenReturn(1);
        final Connection connection = DualConnection
            .builder(connectionProvider, permanentConsistency().build())
            .dualCall(dualCall)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(dualCall, never()).callReplica(any());
        verify(dualCall).callMain(any());
    }


    @Test
    public void shouldUsePrepareNewStatement() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        for (int i = 0; i < 10; i++) {
            connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        }

        assertThat(connectionProvider.getPreparedStatements())
            .hasSize(10);
    }

    @Test
    public void shouldGetMetaDataFromMaster() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.getMetaData();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).getMetaData();
    }

    @Test
    public void shouldStartReadOnlyMode() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.setReadOnly(true);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).setReadOnly(true);
        assertThat(connection.isReadOnly()).isTrue();
    }

    @Test
    public void shouldStopReadOnlyMode() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.setReadOnly(false);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).setReadOnly(false);
        assertThat(connection.isReadOnly()).isFalse();
    }

    @Test
    public void shouldNotSwitchBackToReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.setReadOnly(false);
        connection.setReadOnly(true);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        assertThat(connection.isReadOnly()).isTrue();
    }

    @Test
    public void shouldNotBeReadOnlyByDefault() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        assertThat(connection.isReadOnly()).isFalse();
    }

    @Test
    public void shouldSetCatalog() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final String catalog = "catalog";

        connection.setCatalog(catalog);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).setCatalog(catalog);
        assertThat(connection.getCatalog()).isEqualTo(catalog);
    }

    @Test
    public void shouldGetNullCatalog() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        assertThat(connection.getCatalog()).isNull();
    }

    @Test
    public void shouldSetTransactionIsolationLevel() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();

        connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(connectionProvider.singleProvidedConnection()).setTransactionIsolation(TRANSACTION_SERIALIZABLE);
    }

    @Test
    public void shouldGetTransactionIsolationLevelCallMainDatabase() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();

        connection.getTransactionIsolation();

        verify(connectionProvider.singleProvidedConnection()).getTransactionIsolation();
    }

    @Test
    public void shouldUseSettedTransactionIsolationLevel() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();
        connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);

        connection.getTransactionIsolation();

        assertThat(connectionProvider.getProvidedConnections()).isEmpty();
    }

    @Test
    public void shouldReturnEmptyTypeMap() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();

        assertThat(connection.getTypeMap()).isEmpty();
    }

    @Test
    public void shouldSetTypeMap() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();
        final Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("MyType", Object.class);

        connection.setTypeMap(typeMap);

        assertThat(connection.getTypeMap().keySet()).containsOnly("MyType");
    }

    @Test
    public void shouldReturnTypeMapCopy() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).build();
        final Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("MyType", Object.class);
        connection.setTypeMap(typeMap);

        final Map<String, Class<?>> typeMapFromConnection = connection.getTypeMap();
        typeMapFromConnection.put("AnotherType", Integer.class);

        assertThat(typeMapFromConnection).isNotEqualTo(connection.getTypeMap());
    }

    @Test
    public void shouldSetMapOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("MyType", Object.class);

        connection.setTypeMap(typeMap);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).setTypeMap(typeMap);
    }

    @Test
    public void shouldSetMapOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("MyType", Object.class);

        connection.setTypeMap(typeMap);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).setTypeMap(typeMap);
    }

    @Test
    public void shouldChangeHoldabilityMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connection.getHoldability()).isEqualTo(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        connectionProvider.getProvidedConnections().forEach(conn -> {
            try {
                Mockito.verify(conn).setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldDelegateToMainIfHoldabilityNotKnown() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.getHoldability();

        verify(connectionProvider.singleProvidedConnection()).getHoldability();
    }

    @Test
    public void shouldValidateDelegateToReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.isValid(10);

        verify(connectionProvider.singleProvidedConnection()).isValid(10);
    }

    @Test
    public void shouldValidateDelegateToCurrentConnectionForReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        connection.isValid(10);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).isValid(10);
    }

    @Test
    public void shouldValidateDelegateToCurrentConnectionForMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        connection.isValid(10);
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).isValid(10);
    }

    @Test
    public void shouldCreateArrayOfOnMainConnection() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.createArrayOf("type", new Object[]{});

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).createArrayOf("type", new Object[]{});
    }

    @Test
    public void shouldGetSchema() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.getSchema();

        verify(connectionProvider.singleProvidedConnection()).getSchema();
    }

    @Test
    public void shouldUnwrapConnection() throws SQLException {
        final Connection dualConnection = DualConnection
            .builder(new NoOpConnectionProvider(), permanentConsistency().build())
            .circuitBreaker(null)
            .build();

        final Connection connection = dualConnection.unwrap(Connection.class);

        assertThat(connection).isEqualTo(dualConnection);
    }

    @Test
    public void shouldFailUnwrapInteger() throws SQLException {
        final Connection dualConnection = DualConnection.builder(
            new NoOpConnectionProvider(),
            permanentConsistency().build()
        ).build();

        Throwable thrown = catchThrowable(() -> dualConnection.unwrap(Integer.class));

        assertThat(thrown).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldUnwrapDelegate() throws SQLException {
        final Connection dualConnection = DualConnection.builder(
            new NoOpConnectionProvider(),
            permanentConsistency().build()
        ).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        final NoOpConnection connection = dualConnection.unwrap(NoOpConnection.class);

        assertThat(connection).isNotNull();
    }

    @Test
    public void shouldCheckIfIsWrappedForConnection() throws SQLException {
        final Connection dualConnection = DualConnection.builder(
            new NoOpConnectionProvider(),
            permanentConsistency().build()
        ).build();

        final boolean isWrappedFor = dualConnection.isWrapperFor(Connection.class);

        assertThat(isWrappedFor).isTrue();
    }

    @Test
    public void shouldDelegateCheckIfIsWrappedForUnknownClass() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        dualConnection.isWrapperFor(Integer.class);

        verify(connectionProvider.singleProvidedConnection()).isWrapperFor(Integer.class);
    }

    @Test
    public void shouldDelegateCheckIfIsWrappedFor() throws SQLException {
        final Connection dualConnection = DualConnection.builder(
            new NoOpConnectionProvider(),
            permanentConsistency().build()
        ).build();
        final boolean isWrappedFor = dualConnection.isWrapperFor(NoOpConnection.class);

        assertThat(isWrappedFor).isTrue();
    }

    @Test
    public void shouldNotCloseConnectionForSingleConnectionProvider() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection mock = connectionProvider.getReplicaConnection();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(mock);
        final Connection connection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(connectionProvider.singleProvidedConnection(), never()).close();
    }

    @Test
    public void shouldNotCloseConnectionForSingleConnectionProviderWhenInconsistent() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection mock = connectionProvider.getReplicaConnection();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(mock);
        final Connection connection = DualConnection.builder(
            singleConnectionProvider,
            permanentInconsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(connectionProvider.singleProvidedConnection(), never()).close();
    }

    @Test
    public void shouldNotHideConnectionCloseProblems() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connectionMock = connectionProvider.getReplicaConnection();
        doThrow(new SQLException("Can't Close connection.")).when(connectionMock).close();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connectionMock);
        final Connection connection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        Throwable thrown = catchThrowable(connection::close);

        assertThat(thrown.getCause()).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldPersistReadOnly() throws SQLException {
        final ReadOnlyAwareConnection readOnlyAwareConnection = mock(ReadOnlyAwareConnection.class);
        //noinspection ResultOfMethodCallIgnored
        doCallRealMethod().when(readOnlyAwareConnection).isReadOnly();
        doCallRealMethod().when(readOnlyAwareConnection).setReadOnly(anyBoolean());

        final Connection connection = DualConnection.builder(
            new SingleConnectionProvider(readOnlyAwareConnection),
            permanentConsistency().build()
        ).build();

        connection.setReadOnly(true);
        connection.setReadOnly(false);

        assertThat(readOnlyAwareConnection.isReadOnly()).isFalse();
    }

    @Test
    public void shouldSetSavepoint() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        dualConnection.setSavepoint();

        verify(connectionProvider.singleProvidedConnection()).setSavepoint();
    }

    @Test
    public void shouldSetNamedSavepoint() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        dualConnection.setSavepoint("name");

        verify(connectionProvider.singleProvidedConnection()).setSavepoint("name");
    }

    @Test
    public void shouldRollbackSavepoint() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final Savepoint savepoint = mock(Savepoint.class);

        dualConnection.rollback(savepoint);

        verify(connectionProvider.singleProvidedConnection()).rollback(savepoint);
    }

    @Test
    public void shouldReleaseSavepoint() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final Savepoint savepoint = mock(Savepoint.class);

        dualConnection.releaseSavepoint(savepoint);

        verify(connectionProvider.singleProvidedConnection()).releaseSavepoint(savepoint);
    }

    @Test
    public void shouldReleaseClosedConnection() throws SQLException {
        final Connection connection = new ConnectionProviderMock().getMainConnection();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connection);
        final Connection dualConnection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        dualConnection.close();
        singleConnectionProvider.setConnection(null);

        final Throwable thrown = catchThrowable(() -> dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery());

        assertThat(thrown).isNotNull();
    }

    @Test
    public void shouldCallingCloseOnClosedConnectionBeNoOp() throws SQLException {
        final Connection connection = new ConnectionMock();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connection);
        final Connection dualConnection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        dualConnection.close();

        final Throwable thrown = catchThrowable(dualConnection::close);

        assertThat(thrown).isNull();
    }

    @Test
    public void shouldNotReuseClosedConnection() throws SQLException {
        final Connection connection = new ConnectionMock();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connection);
        final Connection dualConnection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        dualConnection.close();

        final Throwable thrown = catchThrowable(() -> dualConnection.prepareStatement(SIMPLE_QUERY));

        assertThat(thrown).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldClosedConnectionBeNotValid() throws SQLException {
        final Connection connection = new ConnectionMock();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connection);
        final Connection dualConnection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();

        dualConnection.close();

        assertThat(dualConnection.isValid(1)).isFalse();
    }
}
