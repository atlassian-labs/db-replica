package com.atlassian.db.replica.spi;


import java.sql.Connection;

public interface ConnectionProvider {

    boolean isReplicaAvailable();

    /**
     * Provides a connection to the main databse.
     *
     * @return
     */
    Connection getMainConnection();

    /**
     * Provides a connection to a replica databse.
     *
     * @return
     */
    Connection getReplicaConnection();

}
