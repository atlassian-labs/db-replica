package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.internal.ClientInfo;
import com.atlassian.db.replica.internal.ConnectionParameters;
import com.atlassian.db.replica.internal.ForwardCall;
import com.atlassian.db.replica.internal.NetworkTimeout;
import com.atlassian.db.replica.internal.Warnings;
import com.atlassian.db.replica.internal.logs.ConnectionProviderLogger;
import com.atlassian.db.replica.internal.logs.DelegatingLazyLogger;
import com.atlassian.db.replica.internal.NoOpDirtyConnectionCloseHook;
import com.atlassian.db.replica.internal.connection.statements.ReplicaCallableStatement;
import com.atlassian.db.replica.internal.connection.statements.ReplicaPreparedStatement;
import com.atlassian.db.replica.internal.connection.statements.ReplicaStatement;
import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.logs.ReplicaConsistencyLogger;
import com.atlassian.db.replica.internal.logs.TaggedLogger;
import com.atlassian.db.replica.internal.logs.LazyLogger;
import com.atlassian.db.replica.internal.logs.NoopLazyLogger;
import com.atlassian.db.replica.internal.logs.StateAwareLogger;
import com.atlassian.db.replica.internal.connection.state.ConnectionState;
import com.atlassian.db.replica.internal.connection.state.NoOpStateListener;
import com.atlassian.db.replica.internal.connection.state.State;
import com.atlassian.db.replica.internal.connection.state.StateListener;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.DatabaseCall;
import com.atlassian.db.replica.spi.DirtyConnectionCloseHook;
import com.atlassian.db.replica.spi.Logger;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static com.atlassian.db.replica.api.reason.Reason.RO_API_CALL;
import static com.atlassian.db.replica.internal.connection.state.State.CLOSED;
import static com.atlassian.db.replica.internal.connection.state.State.MAIN;
import static java.lang.String.format;

/**
 * Tries to connect to a replica if the query doesn't write to the database.
 * Avoids replicas, which are inconsistent with the main database.
 * Falls back to the main database if it cannot use a replica.
 */
public final class DualConnection implements Connection {
    private static final String CONNECTION_CLOSED_MESSAGE = "This connection has been closed.";
    private final ReplicaConsistency consistency;
    private final DatabaseCall databaseCall;
    private final Set<String> readOnlyFunctions;
    private final DirtyConnectionCloseHook dirtyConnectionCloseHook;
    private final boolean compatibleWithPreviousVersion;
    private final LazyLogger logger;
    private final ConnectionState state;
    private final ConnectionParameters parameters;
    private final Warnings warnings;

    private DualConnection(
        ReplicaConsistency consistency,
        DatabaseCall databaseCall,
        Set<String> readOnlyFunctions,
        DirtyConnectionCloseHook dirtyConnectionCloseHook,
        boolean compatibleWithPreviousVersion,
        LazyLogger logger,
        ConnectionState state,
        ConnectionParameters parameters,
        Warnings warnings
    ) {
        this.dirtyConnectionCloseHook = dirtyConnectionCloseHook;
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
        this.logger = logger;
        this.consistency = consistency;
        this.databaseCall = databaseCall;
        this.readOnlyFunctions = readOnlyFunctions;
        this.state = state;
        this.parameters = parameters;
        this.warnings = warnings;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return ReplicaStatement.builder(
            consistency,
            databaseCall,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).build();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();
        return new ReplicaCallableStatement.Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).build();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        final Connection readConnection = state.getReadConnection(new RouteDecisionBuilder(Reason.RO_API_CALL).sql(
            sql));
        logger.info(() -> format("nativeSQL(sql='%s')", sql));
        return readConnection
            .nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        logger.debug(() -> format("setAutoCommit(autoCommit='%s')", autoCommit));
        checkClosed();
        final boolean autoCommitBefore = getAutoCommit();
        if (autoCommitBefore != autoCommit) {
            preCommit(autoCommitBefore);
        }
        parameters.setAutoCommit(state::getConnection, autoCommit);
        if (autoCommitBefore != getAutoCommit()) {
            recordCommit(autoCommitBefore);
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return parameters.isAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            preCommit(parameters.isAutoCommit());
            logger.debug(() -> "commit()");
            connection.get().commit();
            recordCommit(parameters.isAutoCommit());
        }
    }

    @Override
    public void rollback() throws SQLException {
        logger.debug(() -> "rollback()");
        checkClosed();
        state.clearDirty();
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().rollback();
        }
    }

    @Override
    public void close() throws SQLException {
        if (state.isDirty()) {
            dirtyConnectionCloseHook.onClose(this);
        }
        logger.debug(() -> "close()");
        state.close();
    }

    @Override
    public boolean isClosed() {
        return state.getState().equals(CLOSED);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        parameters.setReadOnly(state::getConnection, readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return parameters.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        parameters.setCatalog(state::getConnection, catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return parameters.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        parameters.setTransactionIsolation(state::getConnection, level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        if (parameters.getTransactionIsolation() != null) {
            //noinspection MagicConstant
            return parameters.getTransactionIsolation();
        } else {
            return state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).getTransactionIsolation();
        }

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            warnings.saveWarning(connection.get().getWarnings());
        }
        return warnings.getWarning();
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().clearWarnings();
        }
        warnings.clear();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return ReplicaStatement
            .builder(
                consistency,
                databaseCall,
                readOnlyFunctions,
                this,
                compatibleWithPreviousVersion,
                logger,
                state,
                parameters
            )
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency
    ) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return new ReplicaCallableStatement
            .Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        )
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return parameters.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        parameters.setTypeMap(state::getConnection, map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        parameters.setHoldability(state::getConnection, holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return parameters.getHoldability() == null ? state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).getHoldability() : parameters.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        return state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        return state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        checkClosed();
        return ReplicaStatement.builder(
                consistency,
                databaseCall,
                readOnlyFunctions,
                this,
                compatibleWithPreviousVersion,
                logger,
                state,
                parameters
            ).resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .resultSetHoldability(resultSetHoldability)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .resultSetHoldability(resultSetHoldability)
            .build();
    }

    @Override
    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        checkClosed();
        return new ReplicaCallableStatement
            .Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        )
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .resultSetHoldability(resultSetHoldability)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).autoGeneratedKeys(autoGeneratedKeys)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).columnIndexes(columnIndexes)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            consistency,
            databaseCall,
            sql,
            readOnlyFunctions,
            this,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        ).columnNames(columnNames)
            .build();
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }
        return state.getReadConnection(new RouteDecisionBuilder(Reason.RO_API_CALL)).isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            checkClosed();
        } catch (final SQLException cause) {
            final Map<String, ClientInfoStatus> failures = new HashMap<>();
            failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
            throw new SQLClientInfoException(CONNECTION_CLOSED_MESSAGE, failures, cause);
        }
        try {
            parameters.setClientInfo(state::getConnection, new ClientInfo(name, value));
        } catch (SQLException cause) {
            final Map<String, ClientInfoStatus> failures = new HashMap<>();
            failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
            throw new SQLClientInfoException(CONNECTION_CLOSED_MESSAGE, failures, cause);
        }

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            checkClosed();
        } catch (final SQLException cause) {
            final Map<String, ClientInfoStatus> failures = new HashMap<>();
            for (Map.Entry<Object, Object> e : properties.entrySet()) {
                failures.put((String) e.getKey(), ClientInfoStatus.REASON_UNKNOWN);
            }
            throw new SQLClientInfoException(CONNECTION_CLOSED_MESSAGE, failures, cause);
        }
        try {
            parameters.setClientInfo(state::getConnection, new ClientInfo(properties));
        } catch (SQLException cause) {
            final Map<String, ClientInfoStatus> failures = new HashMap<>();
            for (Map.Entry<Object, Object> e : properties.entrySet()) {
                failures.put((String) e.getKey(), ClientInfoStatus.REASON_UNKNOWN);
            }
            throw new SQLClientInfoException(CONNECTION_CLOSED_MESSAGE, failures, cause);
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        return state
            .getReadConnection(new RouteDecisionBuilder(Reason.RO_API_CALL))
            .createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        parameters.setSchema(state::getConnection, schema);
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return state.getReadConnection(new RouteDecisionBuilder(Reason.RO_API_CALL)).getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        state.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        parameters.setNetworkTimeout(state::getConnection, new NetworkTimeout(executor, milliseconds));
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return state
            .getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL))
            .getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        final Connection currentConnection = state.getReadConnection(new RouteDecisionBuilder(RO_API_CALL));
        if (iface.isAssignableFrom(currentConnection.getClass())) {
            return iface.cast(currentConnection);
        } else {
            return currentConnection.unwrap(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return true;
        }
        final Connection currentConnection = state.getReadConnection(new RouteDecisionBuilder(RO_API_CALL));
        if (iface.isAssignableFrom(currentConnection.getClass())) {
            return true;
        } else {
            return currentConnection.isWrapperFor(iface);
        }
    }

    private void recordCommit(boolean autoCommit) throws SQLException {
        if (state.getState().equals(MAIN) && !autoCommit) {
            final Connection mainConnection = state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL));
            consistency.write(mainConnection);
            state.clearDirty();
        }
    }

    private void preCommit(boolean autoCommit) throws SQLException {
        if (state.getState().equals(MAIN) && !autoCommit) {
            final Connection mainConnection = state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL));
            consistency.preCommit(mainConnection);
        }
    }

    public static Builder builder(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency
    ) {
        return new Builder(connectionProvider, consistency);
    }

    public static class Builder {
        private final ConnectionProvider connectionProvider;
        private final ReplicaConsistency consistency;
        private DatabaseCall databaseCall = new ForwardCall();
        private StateListener stateListener = new NoOpStateListener();
        private Set<String> readOnlyFunctions = new HashSet<>();
        private DirtyConnectionCloseHook dirtyConnectionCloseHook = new NoOpDirtyConnectionCloseHook();
        private boolean compatibleWithPreviousVersion = false;
        private Logger logger = null;
        private AtomicReference<ConnectionState> state = new AtomicReference<>();

        private Builder(
            ConnectionProvider connectionProvider,
            ReplicaConsistency consistency
        ) {
            this.connectionProvider = connectionProvider;
            this.consistency = consistency;
        }

        /**
         * Register SQL functions as read-only. It allows utilising replica if the function is invoked.
         *
         * @param functions a collection of read-only function names.
         */
        public DualConnection.Builder readOnlyFunctions(Collection<String> functions) {
            this.readOnlyFunctions = new HashSet<>(functions);
            return this;
        }

        public DualConnection.Builder databaseCall(DatabaseCall databaseCall) {
            this.databaseCall = databaseCall;
            return this;
        }

        DualConnection.Builder stateListener(StateListener stateListener) {
            this.stateListener = stateListener;
            return this;
        }

        /**
         * Puts this connection in compatibility mode with a previous version. Developers can use this method to
         * roll out the new version of the library with a feature flag.
         * <p>
         * It's best-effort, and there's no guarantee the library in compatibility mode will always behave
         * the same way as the previous version of the library.
         */
        public DualConnection.Builder compatibleWithPreviousVersion() {
            this.compatibleWithPreviousVersion = true;
            return this;
        }

        public DualConnection.Builder logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public DualConnection.Builder dirtyConnectionCloseHook(DirtyConnectionCloseHook dirtyConnectionCloseHook) {
            this.dirtyConnectionCloseHook = dirtyConnectionCloseHook;
            return this;
        }

        public Connection build() throws SQLException {
            final LazyLogger lazyLogger = logger != null ?
                new TaggedLogger(
                    "DualConnection", UUID.randomUUID().toString(),
                    new StateAwareLogger(this::getState, new DelegatingLazyLogger(logger))
                ) :
                new NoopLazyLogger();
            final ReplicaConsistency replicaConsistency = logger != null ? new ReplicaConsistencyLogger(
                consistency,
                lazyLogger
            ) : consistency;
            final ConnectionProvider connectionProviderLogger = logger != null ? new ConnectionProviderLogger(
                connectionProvider,
                lazyLogger
            ) : connectionProvider;
            final ConnectionParameters parameters = new ConnectionParameters(lazyLogger);
            final Warnings warnings = new Warnings();
            this.state.set(
                new ConnectionState(
                    connectionProviderLogger,
                    consistency,
                    parameters,
                    warnings,
                    stateListener,
                    lazyLogger
                )
            );
            return new DualConnection(
                replicaConsistency,
                databaseCall,
                readOnlyFunctions,
                dirtyConnectionCloseHook,
                compatibleWithPreviousVersion,
                lazyLogger,
                state.get(),
                parameters,
                warnings
            );
        }

        private State getState() {
            return this.state.get().getState();
        }
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException(CONNECTION_CLOSED_MESSAGE);
        }
    }
}
