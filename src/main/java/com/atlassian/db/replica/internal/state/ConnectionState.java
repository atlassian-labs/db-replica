package com.atlassian.db.replica.internal.state;

import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.internal.ConnectionParameters;
import com.atlassian.db.replica.internal.DecisionAwareReference;
import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.SqlRunnable;
import com.atlassian.db.replica.internal.Warnings;
import com.atlassian.db.replica.internal.logs.LazyLogger;
import com.atlassian.db.replica.internal.logs.TaggedLogger;
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
import static com.atlassian.db.replica.api.reason.Reason.RO_API_CALL;
import static com.atlassian.db.replica.api.reason.Reason.RW_API_CALL;
import static com.atlassian.db.replica.internal.state.State.CLOSED;
import static com.atlassian.db.replica.internal.state.State.COMMITED_MAIN;
import static com.atlassian.db.replica.internal.state.State.MAIN;
import static com.atlassian.db.replica.internal.state.State.NOT_INITIALISED;
import static com.atlassian.db.replica.internal.state.State.REPLICA;
import static java.lang.String.format;

public final class ConnectionState {
    public static final String READ_CONNECTION = "readConnection";
    public static final String WRITE_CONNECTION = "writeConnection";
    private final ReplicaConsistency consistency;
    private volatile Boolean isClosed = false;
    private final ConnectionParameters parameters;
    private final Warnings warnings;
    private final StateListener stateListener;
    private volatile boolean replicaConsistent = true;
    private final DecisionAwareReference<Connection> readConnection;
    private final DecisionAwareReference<Connection> writeConnection;
    private final LazyLogger logger;

    /**
     * When we use a connection to write to the database, it becomes a 'dirty' connection.
     * The state is cleared when we commit the transaction. Currently, we use it only
     * to detect dirty connection close() and commit the transaction before closing
     * the connection.
     * In the future we can use this state to exit `MainConnection` state and move more traffic to replicas.
     * (see <a href="https://github.com/atlassian-labs/db-replica/blob/master/docs/dual-connection-states.png">Dual connection states</a>).
     */
    private volatile boolean dirty = false;

    public void markDirty() {
        logger.debug(() -> "markDirty");
        this.dirty = true;
    }

    public void clearDirty() {
        logger.debug(() -> "clearDirty");
        this.dirty = false;
    }

    public boolean isDirty() {
        return dirty;
    }

    public ConnectionState(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency,
        ConnectionParameters parameters,
        Warnings warnings,
        StateListener stateListener,
        LazyLogger logger
    ) {
        this.consistency = consistency;
        this.parameters = parameters;
        this.warnings = warnings;
        this.stateListener = stateListener;
        this.logger = logger;
        this.readConnection = new DecisionAwareReference<Connection>() {
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
                closeConnection(readConnection, decisionBuilder, READ_CONNECTION);
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
            new RouteDecisionBuilder(RW_API_CALL),
            WRITE_CONNECTION
        ));
        final Optional<SQLException> replicaConnectionCloseException = catchException(() -> closeConnection(
            readConnection,
            new RouteDecisionBuilder(RO_API_CALL),
            READ_CONNECTION
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
        boolean isConsistent;
        try {
            isConsistent = consistency.isConsistent(() -> readConnection.get(decisionBuilder));
        } catch (Exception e) {
            closeConnection(readConnection, decisionBuilder, READ_CONNECTION);
            throw e;
        }
        if (isConsistent) {
            if (getState().equals(COMMITED_MAIN)) {
                closeConnection(writeConnection, decisionBuilder, WRITE_CONNECTION);
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
        RouteDecisionBuilder decisionBuilder,
        String context
    ) throws SQLException {
        final TaggedLogger closeConnectionLogger = new TaggedLogger("connectionType", context, logger);
        closeConnectionLogger.debug(() -> "Closing connection");
        if (!connectionReference.isInitialized()) {
            closeConnectionLogger.debug(() -> "Connection was not initialized");
            return;
        }
        final Connection connection = connectionReference.get(decisionBuilder);
        if (connection.isClosed()) {
            closeConnectionLogger.debug(() -> "Connection was already closed");
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
                closeConnectionLogger.debug(() -> format("Closing connection(%s) setAutoCommit(true)", connection));
                connection.setAutoCommit(true);
                closeConnectionLogger.debug(() -> format("Closing connection(%s) setReadOnly(false)", connection));
                connection.setReadOnly(false);
            }
        } finally {
            connectionReference.reset();
            closeConnectionLogger.debug(() -> format("Closing connection(%s) close()", connection));
            connection.close();
        }
    }

}
