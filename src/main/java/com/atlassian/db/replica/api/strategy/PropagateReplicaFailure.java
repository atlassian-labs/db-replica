package com.atlassian.db.replica.api.strategy;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.ReplicaFailureStrategy;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Propagates replica failure.
 */
public class PropagateReplicaFailure implements ReplicaFailureStrategy {
    @Override
    public Connection onFailure(SQLException exception, SqlCall<Connection> mainConnection) throws SQLException {
        throw exception;
    }
}
