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

}
