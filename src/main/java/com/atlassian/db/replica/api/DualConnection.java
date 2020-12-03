package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.circuitbreaker.BreakerState;
import com.atlassian.db.replica.internal.*;
import com.atlassian.db.replica.internal.circuitbreaker.BreakOnNotSupportedOperations;
import com.atlassian.db.replica.internal.circuitbreaker.BreakerConnection;
import com.atlassian.db.replica.internal.circuitbreaker.BreakerHandler;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.DualCall;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Uses main database connections for UPDATE, INSERT and delete queries.
 * Uses replica connection for SELECT queries if the replica is in sync with the main database.
 */
public class DualConnection implements Connection {
    private final ReplicaConnectionProvider connectionProvider;
    private final ReplicaConsistency consistency;
    private final DualCall dualCall;

    private DualConnection(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency,
        DualCall dualCall
    ) {
        this.connectionProvider = new ReplicaConnectionProvider(connectionProvider, consistency);
        this.consistency = consistency;
        this.dualCall = dualCall;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return ReplicaStatement.builder(connectionProvider, consistency, dualCall).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(connectionProvider, consistency, dualCall, sql).build();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();
        return new ReplicaCallableStatement.Builder(connectionProvider, consistency, dualCall, sql).build();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return connectionProvider.getWriteConnection().nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        connectionProvider.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return connectionProvider.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        connectionProvider.commit();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        connectionProvider.rollback();
    }

    @Override
    public void close() throws SQLException {
        connectionProvider.close();
    }

    @Override
    public boolean isClosed() {
        return connectionProvider.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return connectionProvider.getWriteConnection().getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        connectionProvider.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return connectionProvider.getReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        connectionProvider.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return connectionProvider.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        connectionProvider.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        //noinspection MagicConstant
        return connectionProvider.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return connectionProvider.getWarning();
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        connectionProvider.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return ReplicaStatement
            .builder(connectionProvider, consistency, dualCall)
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
            connectionProvider,
            consistency,
            dualCall,
            sql
        ).resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return new ReplicaCallableStatement
            .Builder(connectionProvider, consistency, dualCall, sql)
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return connectionProvider.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        connectionProvider.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        connectionProvider.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return connectionProvider.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        return connectionProvider.getWriteConnection().setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        return connectionProvider.getWriteConnection().setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        connectionProvider.getWriteConnection().rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        connectionProvider.getWriteConnection().releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        checkClosed();
        return ReplicaStatement.builder(connectionProvider, consistency, dualCall)
            .resultSetType(resultSetType)
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
            connectionProvider,
            consistency,
            dualCall,
            sql
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
            .Builder(connectionProvider, consistency, dualCall, sql)
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .resultSetHoldability(resultSetHoldability)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            dualCall,
            sql
        ).autoGeneratedKeys(autoGeneratedKeys)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            dualCall,
            sql
        ).columnIndexes(columnIndexes)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            dualCall,
            sql
        ).columnNames(columnNames)
            .build();
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }
        return connectionProvider.getReadConnection().isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            checkClosed();
        } catch (final SQLException cause) {
            final Map<String, ClientInfoStatus> failures = new HashMap<>();
            failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
            throw new SQLClientInfoException("This connection has been closed.", failures, cause);
        }
        throw new ReadReplicaUnsupportedOperationException();
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
            throw new SQLClientInfoException("This connection has been closed.", failures, cause);
        }
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        return connectionProvider.getWriteConnection().createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return connectionProvider.getReadConnection().getSchema();
    }

    @Override
    public void abort(Executor executor) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        return connectionProvider.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return true;
        }
        return connectionProvider.isWrapperFor(iface);
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
        private DualCall dualCall = new ForwardCall();
        private CircuitBreaker circuitBreaker = new BreakOnNotSupportedOperations();

        private Builder(
            ConnectionProvider connectionProvider,
            ReplicaConsistency consistency
        ) {
            this.connectionProvider = connectionProvider;
            this.consistency = consistency;
        }

        public DualConnection.Builder dualCall(DualCall dualCall) {
            this.dualCall = dualCall;
            return this;
        }

        public DualConnection.Builder circuitBreaker(CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            return this;
        }

        public Connection build() {
            if (circuitBreaker == null) {
                return new DualConnection(
                    connectionProvider,
                    consistency,
                    dualCall
                );
            }
            if (circuitBreaker.getState().equals(BreakerState.OPEN)) {
                return connectionProvider.getMainConnection();
            }
            final BreakerHandler breakerHandler = new BreakerHandler(circuitBreaker);
            final DualConnection dualConnection = new DualConnection(
                connectionProvider,
                consistency,
                dualCall
            );
            return new BreakerConnection(dualConnection, breakerHandler);
        }
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("This connection has been closed.");
        }
    }
}
