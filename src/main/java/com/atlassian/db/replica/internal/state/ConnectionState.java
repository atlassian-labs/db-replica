package com.atlassian.db.replica.internal.state;

import com.atlassian.db.replica.api.state.State;
import com.atlassian.db.replica.internal.ConnectionParameters;
import com.atlassian.db.replica.internal.LazyReference;
import com.atlassian.db.replica.internal.Warnings;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.state.StateListener;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Optional;

import static com.atlassian.db.replica.api.state.State.*;

public final class ConnectionState {
    private final ConnectionProvider connectionProvider;
    private final ReplicaConsistency consistency;
    private volatile Boolean isClosed = false;
    private final ConnectionParameters parameters;
    private final Warnings warnings;
    private final StateListener stateListener;

    private final LazyReference<Connection> readConnection = new LazyReference<Connection>() {
        @Override
        protected Connection create() throws SQLException {
            if (connectionProvider.isReplicaAvailable()) {
                return connectionProvider.getReplicaConnection();
            } else {
                return getWriteConnection();
            }
        }
    };

    private final LazyReference<Connection> writeConnection = new LazyReference<Connection>() {
        @Override
        protected Connection create() throws SQLException {
            return connectionProvider.getMainConnection();
        }
    };

    public ConnectionState(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency,
        ConnectionParameters parameters,
        Warnings warnings,
        StateListener stateListener
    ) {
        this.connectionProvider = connectionProvider;
        this.consistency = consistency;
        this.parameters = parameters;
        this.warnings = warnings;
        this.stateListener = stateListener;
    }

    public State getState() {
        if (isClosed != null && isClosed) {
            return CLOSED;
        } else {
            final boolean readReady = readConnection.isInitialized();
            final boolean writeReady = writeConnection.isInitialized();
            if (!readReady && !writeReady) {
                return NOT_INITIALISED;
            } else if (writeReady) {
                return MAIN;
            } else {
                return REPLICA;
            }
        }
    }

    public Optional<Connection> getConnection() {
        final State state = getState();
        if (state.equals(REPLICA)) {
            return Optional.of(this.readConnection.get());
        } else if (state.equals(MAIN)) {
            return Optional.of(this.writeConnection.get());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Provides a connection that will be used for reading operation. Will use read-replica if possible.
     */
    public Connection getReadConnection() throws SQLException {
        final State stateBefore = getState();
        final Connection connection = prepareReadConnection();
        final State stateAfter = getState();
        if (!stateAfter.equals(stateBefore)) {
            stateListener.transition(stateBefore, stateAfter);
        }
        return connection;
    }

    /**
     * Provides a connection that will be used for writing operation. It will always return a connection to the
     * main database.
     */
    public Connection getWriteConnection() throws SQLException {
        final State stateBefore = getState();
        final Connection connection = prepareMainConnection();
        final State stateAfter = getState();
        if (!stateAfter.equals(stateBefore)) {
            stateListener.transition(stateBefore, stateAfter);
        }
        return connection;
    }

    private Connection prepareMainConnection() throws SQLException {
        if (getState().equals(MAIN)) {
            return writeConnection.get();
        }

        final Optional<Connection> connection = getConnection();
        if (connection.isPresent() && connection.get().equals(writeConnection.get())) {
            readConnection.reset();
            parameters.initialize(writeConnection.get());
        } else {
            closeConnection(readConnection);
            parameters.initialize(writeConnection.get());
        }
        return writeConnection.get();
    }

    public void close() throws SQLException {
        final State state = getState();
        isClosed = true;
        if (state.equals(MAIN)) {
            closeConnection(writeConnection);
        } else if (state.equals(REPLICA)) {
            closeConnection(readConnection);
        }
        final State stateAfter = getState();
        if (!stateAfter.equals(state)) {
            stateListener.transition(state, stateAfter);
        }
    }

    /**
     * Provides a connection that will be used for reading operation. Will use read-replica if possible.
     */
    private Connection prepareReadConnection() throws SQLException {
        if (parameters.getTransactionIsolation() != null && parameters.getTransactionIsolation() > Connection.TRANSACTION_READ_COMMITTED) {
            return getWriteConnection();
        }
        if (getState().equals(MAIN)) {
            return writeConnection.get();
        }
        final boolean isNotInitialised = getState().equals(NOT_INITIALISED);
        if (consistency.isConsistent(readConnection)) {
            if (isNotInitialised) {
                parameters.initialize(readConnection.get());
            }
            return readConnection.get();
        } else {
            return getWriteConnection();
        }
    }

    private void closeConnection(LazyReference<Connection> connectionReference) throws SQLException {
        try {
            if (!connectionReference.isInitialized()) {
                return;
            }
            final Connection connection = connectionReference.get();
            try {
                warnings.saveWarning(connection.getWarnings());
            } catch (Exception e) {
                warnings.saveWarning(new SQLWarning(e));
            }
            connection.close();
        } finally {
            connectionReference.reset();
        }
    }
}
