package com.atlassian.db.replica.spi;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionProvider {

    boolean isReplicaAvailable();

    /**
     * @return a connection to the main databsase
     */
    Connection getMainConnection() throws SQLException;

    /**
     * @return a connection to a replica database
     */
    Connection getReplicaConnection() throws SQLException;
}
