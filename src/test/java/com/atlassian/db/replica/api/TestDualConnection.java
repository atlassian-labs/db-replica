package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.CircularConsistency;
import com.atlassian.db.replica.api.mocks.ConnectionMock;
import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.NoOpConnection;
import com.atlassian.db.replica.api.mocks.NoOpConnectionProvider;
import com.atlassian.db.replica.api.mocks.ReadOnlyAwareConnection;
import com.atlassian.db.replica.api.mocks.SingleConnectionProvider;
import com.atlassian.db.replica.internal.MonotonicMemoryCache;
import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.state.State;
import com.atlassian.db.replica.internal.state.StateListener;
import com.atlassian.db.replica.spi.DatabaseCall;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.NotImplementedException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.atlassian.db.replica.api.Queries.LARGE_SQL_QUERY;
import static com.atlassian.db.replica.api.Queries.SELECT_FOR_KEY_SHARE;
import static com.atlassian.db.replica.api.Queries.SELECT_FOR_NO_KEY_UPDATE;
import static com.atlassian.db.replica.api.Queries.SELECT_FOR_SHARE;
import static com.atlassian.db.replica.api.Queries.SELECT_FOR_UPDATE;
import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentConsistency;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentInconsistency;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.MAIN;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.REPLICA;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class TestDualConnection {

    @Test
    public void shouldUseMainConnectionForInserts() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement("insert into \"public\".\"fvv\" (\"cd\", \"iid\", \"did\", \"s\")\n" +
            "values (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)\n" +
            "ON CONFLICT (\"ssid\", \"iid\") DO UPDATE SET \"s\" = \"fvv\".\"s\" + ?, \"ssid\" = \"fvv\".\"ssid\", \"us\" = \"es\".\"us\", \"iid\" = \"fvv\".\"iid\", \"cd\" = \"es\".\"cd\" WHERE fvv.cd is distinct from es. cd\n" +
            "returning \"fvv\".\"id\", \"fvv\".\"ssid\", \"fvv\".\"iid\", \"fvv\".\"cd\", \"fvv\".\"s\", \"fvv\".\"created\", \"fvv\".\"us\"").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

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
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        when(databaseCall.call(any(), any())).thenReturn(true);
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).databaseCall(databaseCall).build();

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

        verify(databaseCall, times(6)).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.RW_API_CALL).sql(SIMPLE_QUERY).build())
        );
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
    public void shouldRunNativeSqlOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.nativeSQL(SIMPLE_QUERY);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA);
    }

    @Test
    public void shouldUseMainConnectionForSelectForUpdate() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SELECT_FOR_UPDATE).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForSelectForNoKeyUpdate() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SELECT_FOR_NO_KEY_UPDATE).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForSelectForShare() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SELECT_FOR_SHARE).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForSelectForNoKeyShare() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SELECT_FOR_KEY_SHARE).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldLockForSelectForUpdate() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final Connection connection = DualConnection.builder(
                connectionProvider,
                permanentConsistency().build()
            ).databaseCall(databaseCall)
            .build();

        connection.prepareStatement(SELECT_FOR_UPDATE).executeQuery();
        verify(databaseCall).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.LOCK).sql(SELECT_FOR_UPDATE).build())
        );
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
    public void shouldUseReplicaForKnownReadOnlyFunctionCalls() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement("SELECT count(*) FROM user").executeQuery();
        connection.prepareStatement("SELECT COUNT(*) FROM user").executeQuery();
        connection.prepareStatement("SELECT max(user.id) FROM user").executeQuery();
        connection.prepareStatement("SELECT MAX(user.id) FROM user").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldSupprtCustomReadOnlyFunctions() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
                connectionProvider,
                permanentConsistency().build()
            ).readOnlyFunctions(ImmutableSet.of("myFunction"))
            .build();

        connection.prepareStatement("SELECT myFunction() FROM user").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldDetectWriteOperationForSqlFunction() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final Connection connection = DualConnection.builder(
                connectionProvider,
                permanentConsistency().build()
            ).databaseCall(databaseCall)
            .build();

        final String sql = "SELECT doSomething(1234)";

        connection.prepareStatement(sql).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(databaseCall).call(any(), eq(new RouteDecisionBuilder(Reason.WRITE_OPERATION).sql(sql).build()));
    }

    @Test
    public void shouldUseReplicaForComplexQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(
            "select \"ssid_1\" from ((select \"PROJECT_3\".\"id\" as \"ssid_1\" from \"public\".\"project\" \"PROJECT_3\" join \"public\".\"schemepermissions\" \"SCHEME_PERMISSIONS_4\" on \"PROJECT_3\".\"permissionscheme\" = \"SCHEME_PERMISSIONS_4\".\"scheme\" join \"public\".\"projectroleactor\" \"PROJECT_ROLE_ACTOR_5\" on \"SCHEME_PERMISSIONS_4\".\"perm_parameter\" = cast(\"PROJECT_ROLE_ACTOR_5\".\"projectroleid\" as varchar) and \"PROJECT_3\".\"id\" = \"PROJECT_ROLE_ACTOR_5\".\"pid\" where \"SCHEME_PERMISSIONS_4\".\"permission_key\" = $1").executeQuery();

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
            "set \"s\" = \"ISSUE\".\"s\" + 1\n" +
            "where \"ISSUE\".\"id\" in (42, 43) returning \"ISSUE\".\"id\", \"ISSUE\".\"s\"").executeQuery();

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
            "set \"s\" = \"ISSUE\".\"s\" + 1\n" +
            "where \"ISSUE\".\"id\" in (42, 43) RETURNING \"ISSUE\".\"id\", \"ISSUE\".\"s\"").executeQuery();

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
            "where \"ISSUE\".\"id\" in (42, 43) returning \"ISSUE\".\"id\", \"ISSUE\".\"s\"").executeQuery();

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
            "where \"ISSUE\".\"id\" in (42, 43) RETURNING \"ISSUE\".\"id\", \"ISSUE\".\"s\"").executeQuery();

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
    public void shouldExecuteReadOperationOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final Connection connection = DualConnection
            .builder(connectionProvider, permanentConsistency().build())
            .databaseCall(databaseCall)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(databaseCall).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.READ_OPERATION).sql(SIMPLE_QUERY).build())
        );
    }

    @Test
    public void shouldExecuteOnMainWhenNotConsistent() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final ReplicaConsistency consistency = permanentInconsistency().build();
        final Connection connection = DualConnection
            .builder(connectionProvider, consistency)
            .databaseCall(databaseCall)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA, MAIN);
        verify(databaseCall).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.REPLICA_INCONSISTENT).sql(SIMPLE_QUERY).build())
        );
    }

    @Test
    public void shouldExecuteUpdateOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        when(databaseCall.call(any(), any())).thenReturn(1);
        final Connection connection = DualConnection
            .builder(connectionProvider, permanentConsistency().build())
            .databaseCall(databaseCall)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(databaseCall).call(any(), eq(new RouteDecisionBuilder(Reason.RW_API_CALL).sql(SIMPLE_QUERY).build()));
    }

    @Test
    public void shouldReuseMainConnection() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        when(databaseCall.call(any(), any())).thenReturn(1);
        final Connection dualConnection = DualConnection.builder(
                connectionProvider,
                permanentConsistency().build()
            ).databaseCall(databaseCall)
            .build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        Mockito.reset(databaseCall);

        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(databaseCall).call(
            any(),
            eq(
                new RouteDecisionBuilder(Reason.MAIN_CONNECTION_REUSE)
                    .sql(SIMPLE_QUERY)
                    .cause(
                        new RouteDecisionBuilder(Reason.RW_API_CALL).sql(SIMPLE_QUERY).build()
                    )
                    .build()
            )
        );
    }

    @Test
    public void shouldShowThatReadRunAfterWriteIsRunOnMainNotReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final StateListener stateListener = mock(StateListener.class);
        when(databaseCall.call(any(), any())).thenReturn(mock(java.sql.ResultSet.class));
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).databaseCall(databaseCall).stateListener(stateListener).build();


        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        when(databaseCall.call(any(), any())).thenReturn(true);
        dualConnection.prepareStatement(SIMPLE_QUERY).execute();
        Mockito.reset(databaseCall);
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();


        verify(stateListener).transition(State.NOT_INITIALISED, State.REPLICA);
        verify(stateListener).transition(State.REPLICA, State.MAIN);
        verifyNoMoreInteractions(stateListener);
        verify(databaseCall).call(
            any(),
            eq(
                new RouteDecisionBuilder(Reason.MAIN_CONNECTION_REUSE)
                    .sql(SIMPLE_QUERY)
                    .cause(
                        new RouteDecisionBuilder(Reason.RW_API_CALL).sql(SIMPLE_QUERY).build()
                    )
                    .build()
            )
        );
    }

    @Test
    public void shouldReuseMainConnectionForNoneWriteAfterInconsistencyWrite() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final StateListener stateListener = mock(StateListener.class);
        when(databaseCall.call(any(), any())).thenReturn(mock(java.sql.ResultSet.class));
        final Connection dualConnection = DualConnection.builder(
            connectionProvider,
            new CircularConsistency.Builder(ImmutableList.of(true, false)).build()
        ).databaseCall(databaseCall).stateListener(stateListener).build();


        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        when(databaseCall.call(any(), any())).thenReturn(true);
        dualConnection.prepareStatement(SIMPLE_QUERY).execute();
        Mockito.reset(databaseCall);
        when(databaseCall.call(any(), any())).thenReturn(mock(ResultSet.class));
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();


        verify(stateListener).transition(State.NOT_INITIALISED, State.REPLICA);
        verify(stateListener).transition(State.REPLICA, State.COMMITED_MAIN);
        verify(stateListener).transition(State.COMMITED_MAIN, State.MAIN);
        verifyNoMoreInteractions(stateListener);
        verify(databaseCall).call(
            any(),
            eq(
                new RouteDecisionBuilder(Reason.MAIN_CONNECTION_REUSE)
                    .sql(SIMPLE_QUERY)
                    .cause(
                        new RouteDecisionBuilder(Reason.REPLICA_INCONSISTENT).sql(SIMPLE_QUERY).build()
                    )
                    .build()
            )
        );
    }

    @Test
    public void shouldForgiveReplicaIfItCatchesUpOnReads() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final StateListener stateListener = mock(StateListener.class);

        final Connection connection = DualConnection.builder(
                connectionProvider,
                new CircularConsistency.Builder(ImmutableList.of(false, true)).build()
            ).stateListener(stateListener)
            .build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        Mockito.reset(stateListener);

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(stateListener).transition(State.COMMITED_MAIN, State.REPLICA);
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
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

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
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).setReadOnly(false);
        assertThat(connection.isReadOnly()).isFalse();
    }

    @Test
    public void shouldUtiliseReplicaEvenAfterDisablingReadOnly() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.setReadOnly(false);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldNotSwitchBackToReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
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
    public void shouldSetTransactionIsolationLevelForRead() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final Connection connection = DualConnection.builder(
                connectionProvider,
                permanentInconsistency().build()
            ).databaseCall(databaseCall)
            .build();

        connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(connectionProvider.singleProvidedConnection()).setTransactionIsolation(TRANSACTION_SERIALIZABLE);
        verify(databaseCall).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.HIGH_TRANSACTION_ISOLATION_LEVEL).sql(SIMPLE_QUERY).build())
        );
    }

    @Test
    public void shouldSetTransactionIsolationLevelForWrite() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).databaseCall(databaseCall).build();

        connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
        when(databaseCall.call(any(), any())).thenReturn(true);
        connection.prepareStatement(SIMPLE_QUERY).execute();

        verify(connectionProvider.singleProvidedConnection()).setTransactionIsolation(TRANSACTION_SERIALIZABLE);
        verify(databaseCall).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.RW_API_CALL).sql(SIMPLE_QUERY).build())
        );
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

        assertThat(connection.getTypeMap()).containsOnlyKeys("MyType");
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
    public void shouldGetSchemaAndRunUpdate() throws SQLException {
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        when(databaseCall.call(any(), any())).thenReturn(1);
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener).databaseCall(databaseCall).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.getSchema();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(connectionProvider.singleProvidedConnection()).getSchema();
        verify(stateListener).transition(State.NOT_INITIALISED, State.MAIN);

        verify(databaseCall, times(2)).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.RW_API_CALL).sql(SIMPLE_QUERY).build())
        );
    }

    @Test
    public void shouldGetSchemaAndRunQuery() throws SQLException {
        final DatabaseCall databaseCall = mock(DatabaseCall.class);
        when(databaseCall.call(any(), any())).thenReturn(mock(ResultSet.class));
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener).databaseCall(databaseCall).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        verify(stateListener).transition(State.NOT_INITIALISED, State.REPLICA);
        connection.getSchema();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        verifyNoMoreInteractions(stateListener);
        verify(connectionProvider.singleProvidedConnection()).getSchema();

        verify(databaseCall, times(2)).call(
            any(),
            eq(new RouteDecisionBuilder(Reason.READ_OPERATION).sql(SIMPLE_QUERY).build())
        );
    }

    @Test
    public void shouldUnwrapConnection() throws SQLException {
        final Connection dualConnection = DualConnection
            .builder(new NoOpConnectionProvider(), permanentConsistency().build())
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
    public void shouldCloseReplicaConnectionWhenConsistencyCheckThrowsException() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection mock = connectionProvider.getReplicaConnection();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(mock);
        final String exceptionMessage = "Can't check replica consistency";
        final Connection connection = DualConnection.builder(
            singleConnectionProvider,
            new ReplicaConsistency() {
                @Override
                public void write(Connection main) {
                    throw new NotImplementedException();
                }

                @Override
                public boolean isConsistent(Supplier<Connection> replica) {
                    replica.get();
                    throw new RuntimeException(exceptionMessage);
                }
            }
        ).build();

        final Throwable throwable = catchThrowable(() -> connection.prepareStatement(SIMPLE_QUERY).executeQuery());

        assertThat(throwable).hasMessageContaining(exceptionMessage);
        verify(connectionProvider.singleProvidedConnection()).close();
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

        assertThat(thrown).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldPersistReadOnly() throws SQLException {
        final ReadOnlyAwareConnection readOnlyAwareConnection = mock(ReadOnlyAwareConnection.class);
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

    @Test
    public void shouldKeepUsingReplicaAfterSettingTimeout() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        try (Statement statement = connection.createStatement()) {
            statement.execute("set statement_timeout to 30000");
        }

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldKeepStatementTimeoutWhenSwitchingConnections() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        try (Statement statement = connection.createStatement()) {
            statement.execute("set statement_timeout to 30000");
        }

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        final List<Statement> replicaStatements = connectionProvider.getStatements();
        connection.prepareStatement(SELECT_FOR_UPDATE).executeQuery();
        connection.prepareStatement(SELECT_FOR_UPDATE).executeQuery();

        final List<Statement> allStatements = connectionProvider.getStatements();
        final List<Statement> mainConnectionStatements = allStatements
            .stream()
            .filter(statement -> !replicaStatements.contains(statement))
            .collect(Collectors.toList());
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        assertThat(replicaStatements).hasSize(1);
        verify(replicaStatements.get(0)).execute("set statement_timeout to 30000");
        assertThat(mainConnectionStatements).hasSize(1);
        verify(mainConnectionStatements.get(0)).execute("set statement_timeout to 30000");
    }

    @Test
    public void shouldUpdateReadOnlyStateImmediately() throws SQLException {
        final Connection connection = new ConnectionMock();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connection);
        final Connection dualConnection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.setReadOnly(true);
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        dualConnection.setReadOnly(false);

        Assertions.assertThat(connection.isReadOnly()).isFalse();
    }

    @Test
    public void shouldResetReadOnlyModeBeforeReleasingConnection() throws SQLException {
        final Connection connection = new ConnectionMock();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connection);
        final Connection dualConnection = DualConnection.builder(
            singleConnectionProvider,
            permanentConsistency().build()
        ).build();
        dualConnection.setReadOnly(true);
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();

        dualConnection.close();

        Assertions.assertThat(connection.isReadOnly()).isFalse();
    }

    @Test
    public void shouldUseReplicaForCreateArrayOf() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).getConnection().createArrayOf("int8", Arrays.array(100L));

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldPropagateLastWrite() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final MonotonicMemoryCache<Instant> lastWriteCache = new MonotonicMemoryCache<>();

        final Connection connection = DualConnection.builder(
            connectionProvider,
            new PessimisticPropagationConsistency
                .Builder()
                .assumeMaxPropagation(Duration.ofDays(10))
                .cacheLastWrite(lastWriteCache)
                .build()
        ).dirtyConnectionCloseHook(Connection::commit)
            .build();
        connection.setAutoCommit(false);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.close();

        assertThat(lastWriteCache.get()).isPresent();
    }

    @Test
    public void shouldSetEnforceOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().ignoreSupplier(true).build()
        ).build();

        connection.createStatement().execute("set mcrud.enforce=1");

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }
}
