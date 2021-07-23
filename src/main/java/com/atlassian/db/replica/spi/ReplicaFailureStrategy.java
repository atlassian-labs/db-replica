package com.atlassian.db.replica.spi;


import com.atlassian.db.replica.api.SqlCall;

import java.sql.Connection;
import java.sql.SQLException;

public interface ReplicaFailureStrategy {
    /**
     * Controls replica failures.
     */
    Connection onFailure(SQLException exception, SqlCall<Connection> mainConnection) throws SQLException;
}
