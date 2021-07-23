package com.atlassian.db.replica.api.strategy;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.ReplicaFailureStrategy;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Usees main connection after replica failure.
 */
public class FallBackToMain implements ReplicaFailureStrategy {

    @Override
    public Connection onFailure(SQLException exception, SqlCall<Connection> mainConnection) throws SQLException {
        return mainConnection.call();
    }
}
