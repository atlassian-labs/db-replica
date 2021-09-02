package com.atlassian.db.replica.spi;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionProvider extends ReplicaConnectionProvider {

    boolean isReplicaAvailable();

    /**
     * @return a connection to the main database
     */
    Connection getMainConnection() throws SQLException;
}
