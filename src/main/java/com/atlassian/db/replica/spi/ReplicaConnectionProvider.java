package com.atlassian.db.replica.spi;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ReplicaConnectionProvider {
    /**
     * @return a connection to a replica database
     */
    Connection getReplicaConnection() throws SQLException;
}
