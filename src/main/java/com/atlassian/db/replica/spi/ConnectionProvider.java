package com.atlassian.db.replica.spi;


import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionProvider {

    boolean isReplicaAvailable();

    /**
     * Provides a connection to the main databse.
     *
     * @return
     */
    Connection getMainConnection() throws SQLException;

    /**
     * Provides a connection to a replica databse.
     *
     * @return
     */
    Connection getReplicaConnection() throws SQLException;

}
