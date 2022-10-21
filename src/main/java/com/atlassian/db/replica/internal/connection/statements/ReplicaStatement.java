package com.atlassian.db.replica.internal.connection.statements;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.internal.connection.params.ConnectionParameters;
import com.atlassian.db.replica.internal.connection.ReadReplicaUnsupportedOperationException;
import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.SqlQuery;
import com.atlassian.db.replica.internal.connection.statements.operations.Operation;
import com.atlassian.db.replica.internal.connection.statements.operations.Operations;
import com.atlassian.db.replica.internal.dispatcher.StatementDispatcher;
import com.atlassian.db.replica.internal.observability.logs.LazyLogger;
import com.atlassian.db.replica.internal.observability.logs.TaggedLogger;
import com.atlassian.db.replica.internal.connection.state.ConnectionState;
import com.atlassian.db.replica.spi.DatabaseCall;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.atlassian.db.replica.api.reason.Reason.READ_OPERATION;
import static com.atlassian.db.replica.api.reason.Reason.RO_API_CALL;
import static com.atlassian.db.replica.api.reason.Reason.RW_API_CALL;
import static java.lang.String.format;

public class ReplicaStatement implements Statement {
    private volatile boolean isClosed = false;
    private final List<Operation<Statement>> batches = new ArrayList<>();
    private final ReplicaConsistency consistency;
    private final DatabaseCall databaseCall;
    private final DualConnection dualConnection;
    private final boolean compatibleWithPreviousVersion;
    private final LazyLogger logger;
    private final ConnectionState state;
    private final ConnectionParameters parameters;
    private final Operations operations;
    private final StatementDispatcher<? extends Statement> dispatcher;

    ReplicaStatement(
        ReplicaConsistency consistency,
        DatabaseCall databaseCall,
        DualConnection dualConnection,
        boolean compatibleWithPreviousVersion,
        LazyLogger logger,
        ConnectionState state,
        ConnectionParameters parameters,
        Operations operations,
        StatementDispatcher<? extends Statement> dispatcher
    ) {
        this.consistency = consistency;
        this.databaseCall = databaseCall;
        this.dualConnection = dualConnection;
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
        this.logger = logger;
        this.state = state;
        this.parameters = parameters;
        this.operations = operations;
        this.dispatcher = dispatcher;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(READ_OPERATION).sql(sql);
        final Statement statement = dispatcher.getReadStatement(decisionBuilder);
        logger.info(() -> format("executeQuery(sql='%s')", sql));
        return execute(() -> statement.executeQuery(sql), decisionBuilder.build());
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeUpdate(sql='%s')", sql));
        return execute(
            () -> statement.executeUpdate(sql),
            decisionBuilder.build()
        );
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
        dispatcher.close();
        logger.debug(() -> "close()");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return dispatcher.getWriteStatement(new RouteDecisionBuilder(RW_API_CALL)).getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        operations.add(
            (Operation<Statement>) statement -> statement.setMaxFieldSize(max)
        );
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return dispatcher.getWriteStatement(new RouteDecisionBuilder(RW_API_CALL)).getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        operations.add(
            (Operation<Statement>) statement -> statement.setMaxRows(max)
        );
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        operations.add(
            (Operation<Statement>) statement -> statement.setEscapeProcessing(enable)
        );
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        operations.add(
            (Operation<Statement>) statement -> statement.setQueryTimeout(seconds)
        );
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        if (dispatcher.getCurrentStatement() == null) {
            return null;
        } else {
            return dispatcher.getCurrentStatement().getWarnings();
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        if (dispatcher.getCurrentStatement() != null) {
            dispatcher.getCurrentStatement().clearWarnings();
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        dispatcher.getWriteStatement(new RouteDecisionBuilder(RW_API_CALL)).setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder;
        final Statement statement;

        SqlQuery sqlQuery = new SqlQuery(sql, compatibleWithPreviousVersion);
        if (sqlQuery.isSqlSet()) {
            decisionBuilder = new RouteDecisionBuilder(READ_OPERATION).sql(sql);
            statement = dispatcher.getReadStatement(decisionBuilder);
            parameters.addRuntimeParameterConfiguration(sql);
        } else {
            decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
            statement = dispatcher.getWriteStatement(decisionBuilder);
        }

        logger.info(() -> format("execute(sql='%s')", sql));
        return execute(
            () -> statement.execute(sql),
            decisionBuilder.build()
        );
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        if (dispatcher.getCurrentStatement() == null) {
            return null;
        }
        return dispatcher.getCurrentStatement().getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        if (dispatcher.getCurrentStatement() == null) {
            return -1;
        }
        return dispatcher.getCurrentStatement().getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        if (dispatcher.getCurrentStatement() == null) {
            return false;
        }
        return dispatcher.getCurrentStatement().getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        operations.add(
            (Operation<Statement>) statement -> statement.setFetchDirection(direction)
        );
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        operations.add(statement -> statement.setFetchSize(rows));
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        final Operation<Statement> addBatch = statement -> statement.addBatch(sql);
        operations.add(addBatch);
        batches.add(addBatch);
        logger.info(() -> format("addBatch(sql='%s')", sql));
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batches.forEach(operations::remove);
        batches.clear();
        logger.info(() -> "clearBatch()");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> "executeBatch()");
        return execute(statement::executeBatch, decisionBuilder.build());
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        if (dualConnection != null) {
            return dualConnection;
        } else {
            return state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL));
        }
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeUpdate(sql='%s', autoGeneratedKeys)", sql));
        return execute(
            () -> statement.executeUpdate(sql, autoGeneratedKeys),
            decisionBuilder.build()
        );
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeUpdate(sql='%s', columnIndexes)", sql));
        return execute(
            () -> statement.executeUpdate(sql, columnIndexes),
            decisionBuilder.build()
        );
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeUpdate(sql='%s', columnNames)", sql));
        return execute(
            () -> statement.executeUpdate(sql, columnNames),
            decisionBuilder.build()
        );
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("execute(sql='%s', autoGeneratedKeys)", sql));
        return execute(
            () -> statement.execute(sql, autoGeneratedKeys),
            decisionBuilder.build()
        );
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("execute(sql='%s', columnIndexes)", sql));
        return execute(
            () -> statement.execute(sql, columnIndexes),
            decisionBuilder.build()
        );
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("execute(sql='%s', columnNames)", sql));
        return execute(
            () -> statement.execute(sql, columnNames),
            decisionBuilder.build()
        );
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return dispatcher.getCurrentStatement().getResultSetHoldability();
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        operations.add(
            (Operation<Statement>) statement -> statement.setPoolable(poolable)
        );
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        } else if (dispatcher.getCurrentStatement() != null && iface.isAssignableFrom(dispatcher.getCurrentStatement().getClass())) {
            return iface.cast(dispatcher.getCurrentStatement());
        } else if (dispatcher.getCurrentStatement() != null) {
            return dispatcher.getCurrentStatement().unwrap(iface);
        } else {
            throw new SQLException();
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return true;
        } else if (dispatcher.getCurrentStatement() != null && iface.isAssignableFrom(dispatcher.getCurrentStatement().getClass())) {
            return true;
        } else if (dispatcher.getCurrentStatement() != null) {
            return dispatcher.getCurrentStatement().isWrapperFor(iface);
        } else {
            return false;
        }
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkClosed();
        return dispatcher.getCurrentStatement().getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        checkClosed();
        operations.add(
            (Operation<Statement>) statement -> statement.setLargeMaxRows(max)
        );
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        checkClosed();
        return dispatcher.getReadStatement(new RouteDecisionBuilder(RO_API_CALL)).getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> "executeLargeBatch()");
        return execute(
            statement::executeLargeBatch,
            decisionBuilder.build()
        );
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeLargeUpdate(sql='%s')", sql));
        return execute(
            () -> statement.executeLargeUpdate(sql),
            decisionBuilder.build()
        );
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeLargeUpdate(sql='%s', autoGeneratedKeys)", sql));
        return execute(
            () -> statement.executeLargeUpdate(sql, autoGeneratedKeys),
            decisionBuilder.build()
        );
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeLargeUpdate(sql='%s', columnIndexes)", sql));
        return execute(
            () -> statement.executeLargeUpdate(sql, columnIndexes),
            decisionBuilder.build()
        );
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        final RouteDecisionBuilder decisionBuilder = new RouteDecisionBuilder(RW_API_CALL).sql(sql);
        final Statement statement = dispatcher.getWriteStatement(decisionBuilder);
        logger.info(() -> format("executeLargeUpdate(sql='%s', columnNames)", sql));
        return execute(
            () -> statement.executeLargeUpdate(sql, columnNames),
            decisionBuilder.build()
        );
    }

    <T> T execute(final SqlCall<T> call, final RouteDecision routeDecision) throws SQLException {
        final T result = databaseCall.call(call, routeDecision);
        if (routeDecision.mustRunOnMain()) {
            recordWriteAfterQueryExecution();
        }
        return result;
    }

    public static Builder builder(
        ReplicaConsistency consistency,
        DatabaseCall databaseCall,
        Set<String> readOnlyFunctions,
        DualConnection dualConnection,
        boolean compatibleWithPreviousVersion,
        LazyLogger logger,
        ConnectionState state,
        ConnectionParameters parameters
    ) {
        return new Builder(
            consistency,
            databaseCall,
            readOnlyFunctions,
            dualConnection,
            compatibleWithPreviousVersion,
            logger,
            state,
            parameters
        );
    }

    void recordWriteAfterQueryExecution() throws SQLException {
        final Connection connection = dispatcher.getCurrentStatement().getConnection();
        if (connection.getAutoCommit()) {
            consistency.write(connection);
        } else {
            state.markDirty();
        }
    }

    public static class Builder {
        private final ReplicaConsistency consistency;
        private final DatabaseCall databaseCall;
        private final Set<String> readOnlyFunctions;
        private final DualConnection dualConnection;
        private final boolean compatibleWithPreviousVersion;
        private final ConnectionState state;
        private final ConnectionParameters parameters;
        private final LazyLogger logger;
        private Integer resultSetType;
        private Integer resultSetConcurrency;
        private Integer resultSetHoldability;

        private Builder(
            ReplicaConsistency consistency,
            DatabaseCall databaseCall,
            Set<String> readOnlyFunctions,
            DualConnection dualConnection,
            boolean compatibleWithPreviousVersion,
            LazyLogger logger,
            ConnectionState state,
            ConnectionParameters parameters
        ) {
            this.consistency = consistency;
            this.databaseCall = databaseCall;
            this.readOnlyFunctions = readOnlyFunctions;
            this.dualConnection = dualConnection;
            this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
            this.logger = logger;
            this.state = state;
            this.parameters = parameters;
        }

        public Builder resultSetType(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }

        public Builder resultSetConcurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public Builder resultSetHoldability(int resultSetHoldability) {
            this.resultSetHoldability = resultSetHoldability;
            return this;
        }

        public ReplicaStatement build() {
            final Operations operations = new Operations();
            return new ReplicaStatement(
                consistency,
                databaseCall,
                dualConnection,
                compatibleWithPreviousVersion,
                logger.isEnabled() ?
                    new TaggedLogger("ReplicaStatement", UUID.randomUUID().toString(), logger) :
                    logger,
                state,
                parameters,
                operations,
                new StatementDispatcher<>(
                    state,
                    logger,
                    compatibleWithPreviousVersion,
                    readOnlyFunctions,
                    connection -> {
                        if (resultSetType == null) {
                            return connection.createStatement();
                        } else if (resultSetHoldability == null) {
                            return connection.createStatement(resultSetType, resultSetConcurrency);
                        } else {
                            return connection.createStatement(
                                resultSetType,
                                resultSetConcurrency,
                                resultSetHoldability
                            );
                        }
                    },
                    operations
                )
            );
        }
    }

    protected void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("This connection has been closed.");
        }
    }

}