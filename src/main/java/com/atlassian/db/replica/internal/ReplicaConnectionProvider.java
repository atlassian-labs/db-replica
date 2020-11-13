package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.*;
import io.atlassian.util.concurrent.*;

import java.sql.*;

public class ReplicaConnectionProvider implements AutoCloseable {
    private final ReplicaConsistency consistency;
    private final ConnectionProvider connectionProvider;
    private final ResettableLazyReference<Connection> readConnection = new ResettableLazyReference<Connection>() {
        @Override
        protected Connection create() {
            if (connectionProvider.isReplicaAvailable()) {
                return connectionProvider.getReplicaConnection();
            } else {
                return connectionProvider.getMainConnection();
            }
        }
    };

    private final ResettableLazyReference<Connection> writeConnection = new ResettableLazyReference<Connection>() {
        @Override
        protected Connection create() {
            return connectionProvider.getMainConnection();
        }
    };

    public ReplicaConnectionProvider(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency
    ) {
        this.connectionProvider = connectionProvider;
        this.consistency = consistency;
    }

    /**
     * Provides a connection that will be used for reading operation. Will use read-replica if possible.
     *
     * @return
     */
    public Connection getReadConnection() throws SQLException {
        if (consistency.isConsistent(readConnection.get())) {
            return readConnection.get();
        } else {
            readConnection.get().close();
            readConnection.reset();
            return writeConnection.get();
        }
    }

    /**
     * Provides a connection that will be used for writing operation. It will always return a connection to the
     * main database.
     *
     * @return
     */
    public Connection getWriteConnection() {
        return writeConnection.get();
    }

    public Connection getCurrent() {
        if (writeConnection.isInitialized()) {
            return writeConnection.get();
        }
        if (readConnection.isInitialized()) {
            return readConnection.get();
        }
        return writeConnection.get();
    }

    public boolean hasWriteConnection() {
        return writeConnection.isInitialized();
    }

    @Override
    public void close() throws SQLException {
        if (readConnection.isInitialized()) {
            try {
                readConnection.get().close();
                readConnection.reset();
            } catch (Exception e) {
                throw new ReadReplicaUnsupportedOperationException();
            }
        }
        if (writeConnection.isInitialized()) {
            try {
                writeConnection.get().close();
                writeConnection.reset();
            } catch (Exception e) {
                throw new ReadReplicaUnsupportedOperationException();
            }
        }
    }
}
