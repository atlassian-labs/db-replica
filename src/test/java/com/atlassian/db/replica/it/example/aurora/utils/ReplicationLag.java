package com.atlassian.db.replica.it.example.aurora.utils;

import com.atlassian.db.replica.api.SqlCall;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class ReplicationLag {
    private final SqlCall<Connection> connectionSupplier;

    public ReplicationLag(SqlCall<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    public void set(int seconds) throws SQLException {
        try (final Connection connection = connectionSupplier.call()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT aurora_inject_replica_failure(100, " + seconds + ", '');")) {
                preparedStatement.executeQuery();
            }
        }
    }
}
