package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;

public class NoOpConnectionProvider implements ConnectionProvider {

    @Override
    public boolean isReplicaAvailable() {
        return true;
    }

    @Override
    public Connection getMainConnection() {
        return new NoOpConnection();
    }

    @Override
    public Connection getReplicaConnection() {
        return new NoOpConnection();
    }
}
