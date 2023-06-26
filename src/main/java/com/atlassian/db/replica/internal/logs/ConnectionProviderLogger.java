package com.atlassian.db.replica.internal.logs;

import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.String.format;

public final class ConnectionProviderLogger implements ConnectionProvider {
    private final ConnectionProvider delegate;
    private final LazyLogger logger;

    public ConnectionProviderLogger(ConnectionProvider delegate, LazyLogger logger) {
        this.delegate = delegate;
        this.logger = logger;
    }

    @Override
    public boolean isReplicaAvailable() {
        return delegate.isReplicaAvailable();
    }

    @Override
    public Connection getMainConnection() throws SQLException {
        try {
            final Connection mainConnection = delegate.getMainConnection();
            logger.debug(() -> format("ConnectionProvider#getMainConnection(connection=%s)", mainConnection));
            return mainConnection;
        } catch (Exception e) {
            logger.debug(() -> "Failed ConnectionProvider#getMainConnection", e);
            throw e;
        }
    }

    @Override
    public Connection getReplicaConnection() throws SQLException {
        try {
            final Connection replicaConnection = delegate.getReplicaConnection();
            logger.debug(() -> format("ConnectionProvider#getReplicaConnection(connection=%s)", replicaConnection));
            return replicaConnection;
        } catch (Exception e) {
            logger.debug(() -> "Failed ConnectionProvider#getReplicaConnection", e);
            throw e;
        }
    }
}
