package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.*;
import io.atlassian.util.concurrent.*;

import java.sql.*;
import java.util.*;

public class ReplicaConnectionProvider implements AutoCloseable {
    private final ReplicaConsistency consistency;
    private final ConnectionProvider connectionProvider;
    private Set<Connection> initializedConnections = new HashSet<>();
    private Boolean isAutoCommit;
    private Integer transactionIsolation;
    private Boolean isClosed = false;
    private final ResettableLazyReference<Connection> readConnection = new ResettableLazyReference<Connection>() {
        @Override
        protected Connection create() {
            if (connectionProvider.isReplicaAvailable()) {
                return connectionProvider.getReplicaConnection();
            } else {
                return writeConnection.get();
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

    private void initialize(Connection connection) throws SQLException {
        if (!initializedConnections.contains(connection)) {
            if (isAutoCommit != null) {
                connection.setAutoCommit(isAutoCommit);
            }
            if (transactionIsolation != null) {
                connection.setTransactionIsolation(transactionIsolation);
            }
            initializedConnections.add(connection);
        }
    }

    public void setTransactionIsolation(Integer transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
        initializedConnections.clear();
    }

    public void setAutoCommit(Boolean autoCommit) {
        isAutoCommit = autoCommit;
        initializedConnections.clear();
    }

    public Boolean getAutoCommit() {
        return isAutoCommit == null || isAutoCommit;
    }

    public Boolean isClosed() {
        return this.isClosed;
    }

    /**
     * Provides a connection that will be used for reading operation. Will use read-replica if possible.
     *
     * @return
     */
    public Connection getReadConnection() throws SQLException {
        if (transactionIsolation != null && transactionIsolation > Connection.TRANSACTION_READ_COMMITTED) {
            return getWriteConnection();
        }
        final Connection readConnection = this.readConnection.get();
        if (consistency.isConsistent(readConnection)) {
            initialize(readConnection);
            return readConnection;
        } else {
            if (writeConnection.isInitialized() && writeConnection.get().equals(readConnection)) {
                initialize(readConnection);
                return readConnection;
            }
            readConnection.close();
            this.readConnection.reset();
            return getWriteConnection();
        }
    }

    /**
     * Provides a connection that will be used for writing operation. It will always return a connection to the
     * main database.
     *
     * @return
     */
    public Connection getWriteConnection() throws SQLException {
        final Connection connection = writeConnection.get();
        initialize(connection);
        return connection;
    }

    public Connection getCurrent() throws SQLException {
        if (writeConnection.isInitialized()) {
            return getWriteConnection();
        }
        if (readConnection.isInitialized()) {
            return getReadConnection();
        }
        return getWriteConnection();
    }

    public boolean hasWriteConnection() {
        return writeConnection.isInitialized();
    }

    public void rollback() throws SQLException {
        if (writeConnection.isInitialized()) {
            writeConnection.get().rollback();
        }
        if (readConnection.isInitialized()) {
            if (writeConnection.isInitialized() && readConnection.get().equals(writeConnection.get())) {
                return;
            }
            readConnection.get().rollback();
        }
    }

    public void commit() throws SQLException {
        if (writeConnection.isInitialized()) {
            writeConnection.get().commit();
        }
        if (readConnection.isInitialized()) {
            if (writeConnection.isInitialized() && readConnection.get().equals(writeConnection.get())) {
                return;
            }
            readConnection.get().commit();
        }
    }

    @Override
    public void close() throws SQLException {
        Exception lastException = null;
        isClosed = true;
        if (readConnection.isInitialized()) {
            final boolean isWriteAndReadTheSameConnection = writeConnection.isInitialized() && readConnection.get().equals(
                writeConnection.get());
            try {
                readConnection.get().close();
                readConnection.reset();
            } catch (Exception e) {
                lastException = e;
            }
            if (isWriteAndReadTheSameConnection) {
                return;
            }
        }
        if (writeConnection.isInitialized()) {
            try {
                writeConnection.get().close();
                writeConnection.reset();
            } catch (Exception e) {
                lastException = e;
            }
        }
        if (lastException != null) {
            throw new SQLException(lastException);
        }
    }
}
