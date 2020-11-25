package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.NoOpConnection;
import com.atlassian.db.replica.api.mocks.NoOpConnectionProvider;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import com.atlassian.db.replica.api.mocks.PermanentInconsistency;
import com.atlassian.db.replica.api.mocks.SingleConnectionProvider;
import com.atlassian.db.replica.spi.DualCall;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.atlassian.db.replica.api.Queries.LARGE_SQL_QUERY;
import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.MAIN;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.REPLICA;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
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

    @Test
    public void shouldUseReplicaConnectionForExecuteQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldUseReplicaConnectionForExecuteLargeQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(LARGE_SQL_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldUseMainConnectionForWrites() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
    }

    @Test
    public void shouldKeepUsingWriteConnectionAfterFirstWrite() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldNotUseReplicaIfNotAvailable() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final Connection connection = DualConnection.builder(provider, new PermanentInconsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(provider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldSwitchToMainIfNotConsistent() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
    }

    @Test
    public void shouldRunNativeSqlOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();

        connection.nativeSQL(SIMPLE_QUERY);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForSelectForUpdate() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement("SELECT concat_lower_or_upper('Hello', 'World', true)").executeQuery();
        connection.prepareStatement("select concat_lower_or_upper('Hello', 'World')").executeQuery();
        connection.prepareStatement("SELECT NEXTVAL('public.\"AO_21D670_WHITELIST_RULES_ID_seq\"')").executeQuery();
        connection.prepareStatement("SELECT doSomething()").executeQuery();
        connection.prepareStatement("SELECT doSomething(1234)").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForUpdateInExecuteQuery() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement("update \"jiraissue\" \"ISSUE\"\n" +
            "set \"version\" = \"ISSUE\".\"version\" + 1\n" +
            "where \"ISSUE\".\"id\" in (42, 43) RETURNING \"ISSUE\".\"id\", \"ISSUE\".\"version\"").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldUseMainConnectionForUpdateInExecuteQueryUpperCase() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement("UPDATE \"jiraissue\" \"ISSUE\"\n" +
            "set \"version\" = \"ISSUE\".\"version\" + 1\n" +
            "where \"ISSUE\".\"id\" in (42, 43) RETURNING \"ISSUE\".\"id\", \"ISSUE\".\"version\"").executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldGetAutoCommitFalse() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.setAutoCommit(false);

        assertThat(connection.getAutoCommit()).isFalse();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldGetAutoCommitTrue() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.setAutoCommit(true);

        assertThat(connection.getAutoCommit()).isTrue();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldGetAutoCommitDefault() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        assertThat(connection.getAutoCommit()).isTrue();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldDualConnectionCloseSubConnections() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
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
        final Connection dualConnection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
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
        final Connection dualConnection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
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
    public void shouldBeOpenAfterQueryExecution() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connection.isClosed()).isFalse();
    }

    @Test
    public void shouldBeClosedAfterCloseInvoked() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.close();

        assertThat(connection.isClosed()).isTrue();
    }

    @Test
    public void shouldInitiallyOpen() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        assertThat(connection.isClosed()).isFalse();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .isEmpty();
    }

    @Test
    public void shouldNotCloseConnectionAfterExecuteQuery() throws SQLException {
        final ConnectionProviderMock provider = new ConnectionProviderMock(false);
        final Connection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

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
        final Connection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

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
        final Connection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

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
        final Connection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

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
        final Connection dualConnection = DualConnection.builder(provider, new PermanentInconsistency()).build();

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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

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
            .builder(connectionProvider, new PermanentConsistency())
            .dualCall(dualCall)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(dualCall).callReplica(any());
        verify(dualCall, never()).callMain(any());
    }

    @Test
    public void shouldExecuteOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final DualCall dualCall = mock(DualCall.class);
        when(dualCall.callMain(any())).thenReturn(1);
        final Connection connection = DualConnection
            .builder(connectionProvider, new PermanentConsistency())
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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        for (int i = 0; i < 10; i++) {
            connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        }

        assertThat(connectionProvider.getPreparedStatements())
            .hasSize(10);
    }

    @Test
    public void shouldGetMetaDataFromMaster() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.getMetaData();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).getMetaData();
    }

    @Test
    public void shouldStartReadOnlyMode() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.setReadOnly(false);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).setReadOnly(false);
        assertThat(connection.isReadOnly()).isFalse();
    }

    @Test
    public void shouldSetCatalog() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        assertThat(connection.getCatalog()).isNull();
    }

    @Test
    public void shouldSetTransactionIsolationLevel() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();

        connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(connectionProvider.singleProvidedConnection()).setTransactionIsolation(TRANSACTION_SERIALIZABLE);
    }

    @Test
    public void shouldGetTransactionIsolationLevelCallMainDatabase() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();

        connection.getTransactionIsolation();

        verify(connectionProvider.singleProvidedConnection()).getTransactionIsolation();
    }

    @Test
    public void shouldUseSettedTransactionIsolationLevel() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();
        connection.setTransactionIsolation(TRANSACTION_SERIALIZABLE);

        connection.getTransactionIsolation();

        assertThat(connectionProvider.getProvidedConnections()).isEmpty();
    }

    @Test
    public void shouldReturnEmptyTypeMap() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();

        assertThat(connection.getTypeMap()).isEmpty();
    }

    @Test
    public void shouldSetTypeMap() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();
        final Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("MyType", Object.class);

        connection.setTypeMap(typeMap);

        assertThat(connection.getTypeMap().keySet()).containsOnly("MyType");
    }

    @Test
    public void shouldReturnTypeMapCopy() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();
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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

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
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.getHoldability();

        verify(connectionProvider.singleProvidedConnection()).getHoldability();
    }

    @Test
    public void shouldValidateDelegateToReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.isValid(10);

        verify(connectionProvider.singleProvidedConnection()).isValid(10);
    }

    @Test
    public void shouldValidateDelegateToCurrentConnectionForReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        connection.isValid(10);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        verify(connectionProvider.singleProvidedConnection()).isValid(10);
    }

    @Test
    public void shouldValidateDelegateToCurrentConnectionForMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        connection.isValid(10);

        verify(connectionProvider.getProvidedConnections().get(1)).isValid(10);
    }

    @Test
    public void shouldCreateArrayOfOnMainConnection() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.createArrayOf("type", new Object[]{});

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        verify(connectionProvider.singleProvidedConnection()).createArrayOf("type", new Object[]{});
    }

    @Test
    public void shouldGetSchema() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.getSchema();

        verify(connectionProvider.singleProvidedConnection()).getSchema();
    }

    @Test
    public void shouldUnwrapConnection() throws SQLException {
        final Connection dualConnection = DualConnection
            .builder(new NoOpConnectionProvider(), new PermanentConsistency())
            .circuitBreaker(null)
            .build();

        final Connection connection = dualConnection.unwrap(Connection.class);

        assertThat(connection).isEqualTo(dualConnection);
    }

    @Test
    public void shouldFailUnwrapInteger() {
        final Connection dualConnection = DualConnection.builder(new NoOpConnectionProvider(), new PermanentConsistency()).build();

        Throwable thrown = catchThrowable(() -> dualConnection.unwrap(Integer.class));

        assertThat(thrown.getCause()).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldUnwrapDelegate() throws SQLException {
        final Connection dualConnection = DualConnection.builder(new NoOpConnectionProvider(), new PermanentConsistency()).build();
        dualConnection.prepareStatement(SIMPLE_QUERY).executeQuery();
        final NoOpConnection connection = dualConnection.unwrap(NoOpConnection.class);

        assertThat(connection).isNotNull();
    }

    @Test
    public void shouldCheckIfIsWrappedForConnection() throws SQLException {
        final Connection dualConnection = DualConnection.builder(new NoOpConnectionProvider(), new PermanentConsistency()).build();

        final boolean isWrappedFor = dualConnection.isWrapperFor(Connection.class);

        assertThat(isWrappedFor).isTrue();
    }

    @Test
    public void shouldDelegateCheckIfIsWrappedForUnknownClass() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection dualConnection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        dualConnection.isWrapperFor(Integer.class);

        verify(connectionProvider.singleProvidedConnection()).isWrapperFor(Integer.class);
    }

    @Test
    public void shouldDelegateCheckIfIsWrappedFor() throws SQLException {
        final Connection dualConnection = DualConnection.builder(new NoOpConnectionProvider(), new PermanentConsistency()).build();
        final boolean isWrappedFor = dualConnection.isWrapperFor(NoOpConnection.class);

        assertThat(isWrappedFor).isTrue();
    }

    @Test
    public void shouldNotCloseConnectionForSingleConnectionProvider() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection mock = connectionProvider.getReplicaConnection();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(mock);
        final Connection connection = DualConnection.builder(singleConnectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(connectionProvider.singleProvidedConnection(), never()).close();
    }

    @Test
    public void shouldNotCloseConnectionForSingleConnectionProviderWhenInconsistent() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection mock = connectionProvider.getReplicaConnection();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(mock);
        final Connection connection = DualConnection.builder(singleConnectionProvider, new PermanentInconsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(connectionProvider.singleProvidedConnection(), never()).close();
    }

    @Test
    public void shouldNotHideConnectionCloseProblems() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connectionMock = connectionProvider.getReplicaConnection();
        doThrow(new SQLException("Can't Close connection.")).when(connectionMock).close();
        final SingleConnectionProvider singleConnectionProvider = new SingleConnectionProvider(connectionMock);
        final Connection connection = DualConnection.builder(singleConnectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        Throwable thrown = catchThrowable(connection::close);

        assertThat(thrown.getCause()).isInstanceOf(SQLException.class);
    }

}
