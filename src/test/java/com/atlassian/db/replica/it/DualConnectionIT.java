package com.atlassian.db.replica.it;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.mocks.CircularConsistency;
import com.atlassian.db.replica.internal.LsnReplicaConsistency;
import com.atlassian.db.replica.it.consistency.WaitingReplicaConsistency;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.postgresql.jdbc.PgConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

public class DualConnectionIT {

    @Test
    public void shouldUseReplica() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            Connection connection = DualConnection.builder(connectionProvider, new LsnReplicaConsistency()).build();

            try (final ResultSet resultSet = connection.prepareStatement("SELECT 1;").executeQuery()) {
                resultSet.next();
                assertThat(resultSet.getLong(1)).isEqualTo(1);
            }
        }
    }


    @Test
    public void shouldPreserveAutoCommitModeWhileSwitchingFromMainToReplica() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            final Connection connection = DualConnection.builder(
                connectionProvider,
                new CircularConsistency.Builder(ImmutableList.of(false, true)).build()
            ).build();

            connection.setAutoCommit(false);
            connection.prepareStatement("SELECT 1;").executeQuery();
            connection.prepareStatement("SELECT 1;").executeQuery();
            connection.commit();
        }
    }

    @Test
    public void shouldPreserveReadOnlyModeWhileSwitchingFromReplicaToMain() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            final WaitingReplicaConsistency consistency = new WaitingReplicaConsistency(new LsnReplicaConsistency());
            createTable(DualConnection.builder(connectionProvider, consistency).build());
            final Connection connection = DualConnection.builder(
                connectionProvider,
                consistency
            ).build();

            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            final PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO foo(bar) VALUES(?);");
            preparedStatement.setString(1, "test");

            final Throwable throwable = catchThrowable(preparedStatement::executeUpdate);
            final boolean readOnly = connection.isReadOnly();
            connection.close();

            assertThat(readOnly).isTrue();
            assertThat(throwable).hasMessage("ERROR: cannot execute INSERT in a read-only transaction");
        }
    }

    @Test
    public void shouldRunNextValOnMainDatabase() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            final WaitingReplicaConsistency consistency = new WaitingReplicaConsistency(new LsnReplicaConsistency());
            createTestSequence(DualConnection.builder(connectionProvider, consistency).build());
            final Connection connection = DualConnection.builder(connectionProvider, consistency).build();

            connection.prepareStatement("SELECT nextval('test_sequence');").executeQuery();
        }
    }

    @Test
    public void shluldNotFailWhenChangingTransactionIsolationLevel() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            final Connection connection = DualConnection.builder(
                connectionProvider,
                CircularConsistency.permanentConsistency().build()
            ).build();

            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            connection.setAutoCommit(false);
            connection.prepareStatement("SELECT 1;").executeQuery();
            connection.prepareStatement("SELECT 1;").executeQuery();
            connection.commit();
        }
    }

    @SuppressWarnings({"ThrowableNotThrown"})
    @Test
    public void shouldImplementAllConnectionMethods() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            final Connection connection = DualConnection.builder(
                connectionProvider,
                new LsnReplicaConsistency()
            ).build();
            connection.createStatement();
            connection.prepareStatement(SIMPLE_QUERY);
            connection.prepareCall(SIMPLE_QUERY);
            connection.nativeSQL(SIMPLE_QUERY);
            connection.setAutoCommit(true);
            assertThat(connection.getAutoCommit()).isTrue();
            connection.setAutoCommit(false);
            assertThat(connection.getAutoCommit()).isFalse();
            connection.commit();
            connection.rollback();
            connection.isClosed();
            connection.getMetaData();
            connection.setReadOnly(true);
            assertThat(connection.isReadOnly()).isTrue();
            connection.setReadOnly(false);
            assertThat(connection.isReadOnly()).isFalse();
            connection.setCatalog("catalog");
            assertThat(connection.getCatalog()).isEqualTo("catalog");
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.getTransactionIsolation();
            connection.getWarnings();
            connection.clearWarnings();
            connection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
            connection.prepareStatement(SIMPLE_QUERY, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
            connection.prepareCall(SIMPLE_QUERY, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
            connection.getTypeMap();
            connection.setTypeMap(Collections.emptyMap());
            connection.setHoldability(CLOSE_CURSORS_AT_COMMIT);
            connection.getHoldability();
            final Savepoint savepoint = connection.setSavepoint();
            connection.setSavepoint("savepoint");
            connection.rollback(savepoint);
            connection.releaseSavepoint(savepoint);
            connection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
            connection.prepareStatement(SIMPLE_QUERY, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
            connection.prepareCall(SIMPLE_QUERY, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
            connection.prepareStatement(SIMPLE_QUERY, NO_GENERATED_KEYS);
            connection.prepareStatement(SIMPLE_QUERY, new int[]{0});
            connection.prepareStatement(SIMPLE_QUERY, new String[]{"abcd"});
            assertThatThrownBy(connection::createClob).isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(connection::createBlob).isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(connection::createNClob).isInstanceOf(SQLFeatureNotSupportedException.class);
            connection.createSQLXML();
            connection.isValid(30);
            connection.getClientInfo();
            connection.getClientInfo("ApplicationName");
            connection.createArrayOf("float8", new Double[]{21.22});
            assertThatThrownBy(() -> connection.createStruct("float8", null))
                .isInstanceOf(SQLFeatureNotSupportedException.class);
            connection.unwrap(PgConnection.class);
            connection.isWrapperFor(PgConnection.class);

            connection.setSchema("public");
            assertThat(connection.getSchema()).isEqualTo("public");
            connection.setClientInfo("ApplicationName", "app");
            final Properties properties = new Properties();
            properties.setProperty("ApplicationName", "app");
            connection.setClientInfo(properties);
            final int timeout = (int) Duration.ofSeconds(30).toMillis();
            connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), timeout);
            assertThat(connection.getNetworkTimeout()).isEqualTo(timeout);
            connection.abort(Executors.newSingleThreadExecutor());

            connection.close();
        }
    }

    private void createTestSequence(Connection connection) throws SQLException {
        try (final Statement mainStatement = connection.createStatement()) {
            mainStatement.execute("CREATE SEQUENCE test_sequence;");
        }
    }

    private void createTable(Connection connection) throws SQLException {
        try (final Statement mainStatement = connection.createStatement()) {
            mainStatement.execute("CREATE TABLE foo (bar VARCHAR ( 255 ));");
        }
    }

}
