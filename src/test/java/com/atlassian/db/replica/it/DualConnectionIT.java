package com.atlassian.db.replica.it;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.mocks.CircularConsistency;
import com.atlassian.db.replica.internal.LsnReplicaConsistency;
import com.atlassian.db.replica.it.consistency.WaitingReplicaConsistency;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.catchThrowable;

public class DualConnectionIT {

    @Test
    public void shouldUseReplica() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            Connection connection = DualConnection.builder(connectionProvider, new LsnReplicaConsistency()).build();

            try (final ResultSet resultSet = connection.prepareStatement("SELECT 1;").executeQuery()) {
                resultSet.next();
                Assertions.assertThat(resultSet.getLong(1)).isEqualTo(1);
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

            Assertions.assertThat(connection.isReadOnly()).isTrue();
            Assertions.assertThat(throwable).hasMessage("ERROR: cannot execute INSERT in a read-only transaction");
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
