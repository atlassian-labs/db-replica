package com.atlassian.db.replica.it;

import com.atlassian.db.replica.api.*;
import com.atlassian.db.replica.internal.*;
import org.assertj.core.api.*;
import org.junit.*;

import java.sql.*;

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
}
