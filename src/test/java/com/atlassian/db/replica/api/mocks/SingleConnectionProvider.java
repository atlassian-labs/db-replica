package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;

public class SingleConnectionProvider implements ConnectionProvider {
    private Connection connection;

    public SingleConnectionProvider(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean isReplicaAvailable() {
        return true;
    }

    @Override
    public Connection getMainConnection() {
        return connection;
    }

    @Override
    public Connection getReplicaConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
