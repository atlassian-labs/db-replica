package com.atlassian.db.replica.internal.state;

import com.atlassian.db.replica.api.context.RouteDecision;
import com.atlassian.db.replica.api.state.State;
import com.atlassian.db.replica.internal.ConnectionParameters;
import com.atlassian.db.replica.internal.DecisionAwareReference;
import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.Warnings;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.state.StateListener;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Optional;

import static com.atlassian.db.replica.api.context.Reason.HIGH_TRANSACTION_ISOLATION_LEVEL;
import static com.atlassian.db.replica.api.context.Reason.MAIN_CONNECTION_REUSE;
import static com.atlassian.db.replica.api.context.Reason.REPLICA_INCONSISTENT;
import static com.atlassian.db.replica.api.context.Reason.RO_API_CALL;
import static com.atlassian.db.replica.api.context.Reason.RW_API_CALL;
import static com.atlassian.db.replica.api.state.State.CLOSED;
import static com.atlassian.db.replica.api.state.State.COMMITED_MAIN;
import static com.atlassian.db.replica.api.state.State.MAIN;
import static com.atlassian.db.replica.api.state.State.NOT_INITIALISED;
import static com.atlassian.db.replica.api.state.State.REPLICA;

public final class ConnectionState {
    private final ConnectionProvider connectionProvider;
    private final ReplicaConsistency consistency;
    private volatile Boolean isClosed = false;
    private final ConnectionParameters parameters;
    private final Warnings warnings;
    private final StateListener stateListener;
    private volatile boolean replicaConsistent = true;

    private final DecisionAwareReference<Connection> readConnection = new DecisionAwareReference<Connection>() {
        @Override
        public Connection create() throws SQLException {
            if (connectionProvider.isReplicaAvailable()) {
                return connectionProvider.getReplicaConnection();
            } else {
                return getWriteConnection(getDecisionBuilder());
            }
        }
    };

    private final DecisionAwareReference<Connection> writeConnection = new DecisionAwareReference<Connection>() {
        @Override
        public Connection create() throws SQLException {
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
            } else if (!replicaConsistent && writeReady) {
                return COMMITED_MAIN;
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
            return Optional.of(this.readConnection.get(new RouteDecisionBuilder(RO_API_CALL)));
        } else if (hasWriteConnection()) {
            return Optional.of(this.writeConnection.get(new RouteDecisionBuilder(MAIN_CONNECTION_REUSE)));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Provides a connection that will be used for reading operation. Will use read-replica if possible.
     */
    public Connection getReadConnection(RouteDecisionBuilder contextBuilder) throws SQLException {
        final State stateBefore = getState();
        final Connection connection = prepareReadConnection(contextBuilder);
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
    public Connection getWriteConnection(RouteDecisionBuilder decisionBuilder) throws SQLException {
        final State stateBefore = getState();
        replicaConsistent = true;
        final Connection connection = prepareMainConnection(decisionBuilder);
        final State stateAfter = getState();
        if (!stateAfter.equals(stateBefore)) {
            stateListener.transition(stateBefore, stateAfter);
        }
        return connection;
    }

    private Connection prepareMainConnection(RouteDecisionBuilder decisionBuilder) throws SQLException {
        if (hasWriteConnection()) {
            return writeConnection.get(decisionBuilder);
        }
        final Optional<Connection> connection = getConnection();
        if (connection.isPresent() && connection.get().equals(writeConnection.get(decisionBuilder))) {
            readConnection.reset();
            parameters.initialize(writeConnection.get(decisionBuilder));
        } else {
            closeConnection(readConnection, decisionBuilder);
            parameters.initialize(writeConnection.get(decisionBuilder));
        }
        return writeConnection.get(decisionBuilder);
    }

    public Optional<RouteDecision> getDecision() {
        if (getState().equals(MAIN)) {
            return Optional.of(writeConnection.getDecisionBuilder().build());
        } else {
            return Optional.empty();
        }
    }

    public boolean hasWriteConnection() {
        final State state = getState();
        return state.equals(MAIN) || state.equals(COMMITED_MAIN);
    }

    public void close() throws SQLException {
        final State state = getState();
        final boolean haWriteConnection = hasWriteConnection();
        isClosed = true;
        if (haWriteConnection) {
            closeConnection(writeConnection, new RouteDecisionBuilder(RW_API_CALL));
        } else if (state.equals(REPLICA)) {
            closeConnection(readConnection, new RouteDecisionBuilder(RO_API_CALL));
        }
        final State stateAfter = getState();
        if (!stateAfter.equals(state)) {
            stateListener.transition(state, stateAfter);
        }
    }

    /**
     * Provides a connection that will be used for reading operation. Will use read-replica if possible.
     */
    private Connection prepareReadConnection(RouteDecisionBuilder decisionBuilder) throws SQLException {
        if (parameters.getTransactionIsolation() != null && parameters.getTransactionIsolation() > Connection.TRANSACTION_READ_COMMITTED) {
            decisionBuilder.reason(HIGH_TRANSACTION_ISOLATION_LEVEL);
            return prepareMainConnection(decisionBuilder);
        }
        if (getState().equals(MAIN)) {
            decisionBuilder.reason(MAIN_CONNECTION_REUSE);
            decisionBuilder.cause(writeConnection.getDecisionBuilder().build());
            return writeConnection.get(decisionBuilder);
        }
        final boolean isNotInitialised = getState().equals(NOT_INITIALISED);
        if (consistency.isConsistent(() -> readConnection.get(decisionBuilder))) {
            if (getState().equals(COMMITED_MAIN)) {
                closeConnection(writeConnection, decisionBuilder);
            }
            if (isNotInitialised) {
                parameters.initialize(readConnection.get(decisionBuilder));
            }
            replicaConsistent = true;
            return readConnection.get(decisionBuilder);
        } else {
            replicaConsistent = false;
            decisionBuilder.reason(REPLICA_INCONSISTENT);
            return prepareMainConnection(decisionBuilder);
        }
    }

    private void closeConnection(
        DecisionAwareReference<Connection> connectionReference,
        RouteDecisionBuilder decisionBuilder
    ) throws SQLException {
        try {
            if (!connectionReference.isInitialized()) {
                return;
            }
            final Connection connection = connectionReference.get(decisionBuilder);
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
