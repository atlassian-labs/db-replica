package com.atlassian.db.replica.it;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.mocks.CircularConsistency;
import com.atlassian.db.replica.internal.LsnReplicaConsistency;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
    public void shouldPreserveAutoCommitModeWhileSwitchingFromReplicaToMain() throws SQLException {
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
    public void shouldRunNextValOnMainDatabase() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            createTestSequence(connectionProvider);

            final Connection connection = DualConnection.builder(
                connectionProvider,
                CircularConsistency.permanentConsistency().build()
            ).build();

            connection.prepareStatement("SELECT nextval('test_sequence');").executeQuery();
        }
    }

    private void createTestSequence(PostgresConnectionProvider connectionProvider) throws SQLException {
        final Connection mainConnection = connectionProvider.getMainConnection();
        try (final Statement mainStatement = mainConnection.createStatement()) {
            mainStatement.execute("CREATE SEQUENCE test_sequence;");
            final LsnReplicaConsistency consistency = new LsnReplicaConsistency();
            waitForReplicaConsistency(connectionProvider, mainConnection, consistency);
        }
    }

    private void waitForReplicaConsistency(
        PostgresConnectionProvider connectionProvider,
        Connection mainConnection,
        LsnReplicaConsistency consistency
    ) {
        consistency.write(mainConnection);
        for (int i = 0; i < 30; i++) {
            if (consistency.isConsistent(connectionProvider::getReplicaConnection)) {
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Replica is still inconsistent after 30s.");
    }

}
