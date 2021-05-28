package com.atlassian.db.replica.internal.state;

import com.atlassian.db.replica.spi.Chief;
import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.internal.ConnectionParameters;
import com.atlassian.db.replica.internal.DecisionAwareReference;
import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.Warnings;
import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.atlassian.db.replica.api.reason.Reason.HIGH_TRANSACTION_ISOLATION_LEVEL;
import static com.atlassian.db.replica.api.reason.Reason.MAIN_CONNECTION_REUSE;
import static com.atlassian.db.replica.api.reason.Reason.RO_API_CALL;
import static com.atlassian.db.replica.api.reason.Reason.RW_API_CALL;
import static com.atlassian.db.replica.internal.state.State.CLOSED;
import static com.atlassian.db.replica.internal.state.State.COMMITED_MAIN;
import static com.atlassian.db.replica.internal.state.State.MAIN;
import static com.atlassian.db.replica.internal.state.State.NOT_INITIALISED;
import static com.atlassian.db.replica.internal.state.State.REPLICA;
import static java.util.Collections.singleton;

public final class ConnectionState {
    private final ConnectionProvider connectionProvider;
    private volatile Boolean isClosed = false;
    private final ConnectionParameters parameters;
    private final Warnings warnings;
    private final StateListener stateListener;
    private volatile boolean isMainCommited = true;
    private final Chief chief;

    private final DecisionAwareReference<Connection> readConnection = new DecisionAwareReference<Connection>() {
        @Override
        public Connection create() throws SQLException {
            if (connectionProvider.isReplicaAvailable()) {
                final Connection replicaConnection = connectionProvider.getReplicaConnection();
                parameters.initialize(replicaConnection);
                return replicaConnection;
            } else {
                return getWriteConnection(getFirstCause());
            }
        }
    };

    private final DecisionAwareReference<Connection> writeConnection = new DecisionAwareReference<Connection>() {
        @Override
        public Connection create() throws SQLException {
            final Connection mainConnection = connectionProvider.getMainConnection();
            parameters.initialize(mainConnection);
            return mainConnection;
        }
    };

    public ConnectionState(
        ConnectionProvider connectionProvider,
        ConnectionParameters parameters,
        Warnings warnings,
        StateListener stateListener,
        Chief chief
    ) {
        this.connectionProvider = connectionProvider;
        this.parameters = parameters;
        this.warnings = warnings;
        this.stateListener = stateListener;
        this.chief = chief;
    }

    public State getState() {
        if (isClosed != null && isClosed) {
            return CLOSED;
        } else {
            final boolean readReady = readConnection.isInitialized();
            final boolean writeReady = writeConnection.isInitialized();
            if (!readReady && !writeReady) {
                return NOT_INITIALISED;
            } else if (isMainCommited && writeReady) {
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
    public Connection getReadConnection(RouteDecisionBuilder decisionBuilder) throws SQLException {
        final State stateBefore = getState();
        final Connection connection = prepareReadConnection(decisionBuilder);
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
        isMainCommited = false;
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
        } else {
            closeConnection(readConnection, decisionBuilder);
        }
        return writeConnection.get(decisionBuilder);
    }

    public Optional<RouteDecision> getDecision() {
        if (getState().equals(MAIN)) {
            return Optional.of(writeConnection.getFirstCause().build());
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

    public void abort(Executor executor) throws SQLException {
        isClosed = true;
        final Optional<Connection> connection = getConnection();
        if (connection.isPresent()) {
            connection.get().abort(executor);
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
            decisionBuilder.cause(writeConnection.getFirstCause().build());
            return writeConnection.get(decisionBuilder);
        }
        chief.overrideDecision(decisionBuilder, singleton(() -> readConnection.get(decisionBuilder)));
        if (decisionBuilder.build().willRunOnMain()) {
            isMainCommited = true;
            return prepareMainConnection(decisionBuilder);
        } else {
            if (getState().equals(COMMITED_MAIN)) {
                closeConnection(writeConnection, decisionBuilder);
            }
            return readConnection.get(decisionBuilder);
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
            if (connection.isReadOnly()) {
                connection.setAutoCommit(true);
                connection.setReadOnly(false);
            }
            connection.close();
        } finally {
            connectionReference.reset();
        }
    }
}
