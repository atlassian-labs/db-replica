package com.atlassian.db.replica.internal.state;

import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.internal.ConnectionParameters;
import com.atlassian.db.replica.internal.DecisionAwareReference;
import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.SqlRunnable;
import com.atlassian.db.replica.internal.Warnings;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.atlassian.db.replica.api.reason.Reason.HIGH_TRANSACTION_ISOLATION_LEVEL;
import static com.atlassian.db.replica.api.reason.Reason.MAIN_CONNECTION_REUSE;
import static com.atlassian.db.replica.api.reason.Reason.REPLICA_INCONSISTENT;
import static com.atlassian.db.replica.api.reason.Reason.REPLICA_NOT_AVAILABLE;
import static com.atlassian.db.replica.api.reason.Reason.RO_API_CALL;
import static com.atlassian.db.replica.api.reason.Reason.RW_API_CALL;
import static com.atlassian.db.replica.internal.state.State.CLOSED;
import static com.atlassian.db.replica.internal.state.State.COMMITED_MAIN;
import static com.atlassian.db.replica.internal.state.State.MAIN;
import static com.atlassian.db.replica.internal.state.State.NOT_INITIALISED;
import static com.atlassian.db.replica.internal.state.State.REPLICA;

public final class ConnectionState {
    private final ConnectionProvider connectionProvider;
    private final ReplicaConsistency consistency;
    private volatile Boolean isClosed = false;
    private final ConnectionParameters parameters;
    private final Warnings warnings;
    private final StateListener stateListener;
    private volatile boolean replicaConsistent = true;
    private final DecisionAwareReference<Connection> readConnection;
    private final DecisionAwareReference<Connection> writeConnection;
    private final boolean compatibleWithPreviousVersion;

    public ConnectionState(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency,
        ConnectionParameters parameters,
        Warnings warnings,
        StateListener stateListener,
        boolean compatibleWithPreviousVersion
    ) {
        this.connectionProvider = connectionProvider;
        this.consistency = consistency;
        this.parameters = parameters;
        this.warnings = warnings;
        this.stateListener = stateListener;
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
        this.readConnection = new DecisionAwareReference<Connection>() {
            @Override
            public Connection create() throws SQLException {
                return ConnectionState.this.compatibleWithPreviousVersion ? createOld() : createNew();
            }

            private Connection createNew() throws SQLException {
                final Connection replicaConnection = connectionProvider.getReplicaConnection();
                parameters.initialize(replicaConnection);
                return replicaConnection;
            }

            private Connection createOld() throws SQLException {
                if (connectionProvider.isReplicaAvailable()) {
                    final Connection replicaConnection = connectionProvider.getReplicaConnection();
                    parameters.initialize(replicaConnection);
                    return replicaConnection;
                } else {
                    return getWriteConnection(getFirstCause());
                }
            }
        };
        this.writeConnection = new DecisionAwareReference<Connection>() {
            @Override
            public Connection create() throws SQLException {
                final Connection mainConnection = connectionProvider.getMainConnection();
                parameters.initialize(mainConnection);
                return mainConnection;
            }
        };
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
        replicaConsistent = true;
        final Connection connection = prepareMainConnection(decisionBuilder);
        final State stateAfter = getState();
        if (!stateAfter.equals(stateBefore)) {
            stateListener.transition(stateBefore, stateAfter);
        }
        return connection;
    }

    private Connection prepareMainConnection(RouteDecisionBuilder decisionBuilder) throws SQLException {
        final Connection mainDatabaseConnection = this.writeConnection.get(decisionBuilder);
        if (readConnection.isInitialized()) {
            if (readConnection.get(decisionBuilder).equals(mainDatabaseConnection)) {
                readConnection.reset(); // We can release the reference. We still can close it via `writeConnection`
            } else {
                closeConnection(readConnection, decisionBuilder);
            }
        }
        return mainDatabaseConnection;
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
        isClosed = true;
        final Optional<SQLException> mainConnectionCloseException = catchException(() -> closeConnection(
            writeConnection,
            new RouteDecisionBuilder(RW_API_CALL)
        ));
        final Optional<SQLException> replicaConnectionCloseException = catchException(() -> closeConnection(
            readConnection,
            new RouteDecisionBuilder(RO_API_CALL)
        ));
        final State stateAfter = getState();
        if (!stateAfter.equals(state)) {
            stateListener.transition(state, stateAfter);
        }
        throwExceptions(mainConnectionCloseException, replicaConnectionCloseException);
    }

    private Optional<SQLException> catchException(SqlRunnable runnable) {
        try {
            runnable.run();
            return Optional.empty();
        } catch (SQLException e) {
            return Optional.of(e);
        }
    }

    private void throwExceptions(
        Optional<SQLException> mainException,
        Optional<SQLException> replicaException
    ) throws SQLException {
        if (mainException.isPresent() && replicaException.isPresent()) {
            mainException.get().addSuppressed(replicaException.get());
            throw mainException.get();
        } else if (mainException.isPresent()) {
            throw mainException.get();
        } else if (replicaException.isPresent()) {
            throw replicaException.get();
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
        if (!compatibleWithPreviousVersion) {
            if (!connectionProvider.isReplicaAvailable()) {
                decisionBuilder.reason(REPLICA_NOT_AVAILABLE);
                return writeConnection.get(decisionBuilder);
            }
        }
        boolean isConsistent;
        try {
            isConsistent = consistency.isConsistent(() -> readConnection.get(decisionBuilder));
        } catch (Exception e) {
            closeConnection(readConnection, decisionBuilder);
            throw e;
        }
        if (isConsistent) {
            if (getState().equals(COMMITED_MAIN)) {
                closeConnection(writeConnection, decisionBuilder);
            }
            final Connection connection = readConnection.get(decisionBuilder);
            replicaConsistent = true;
            return connection;
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
        if (!connectionReference.isInitialized()) {
            return;
        }
        final Connection connection = connectionReference.get(decisionBuilder);
        if (connection.isClosed()) {
            connectionReference.reset();
            return;
        }
        try {
            try {
                warnings.saveWarning(connection.getWarnings());
            } catch (Exception e) {
                warnings.saveWarning(new SQLWarning(e));
            }
            if (connection.isReadOnly()) {
                connection.setAutoCommit(true);
                connection.setReadOnly(false);
            }
        } finally {
            connectionReference.reset();
            connection.close();
        }
    }

}
