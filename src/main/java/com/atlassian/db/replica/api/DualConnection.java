package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.circuitbreaker.BreakerState;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;
import com.atlassian.db.replica.impl.ForwardCall;
import com.atlassian.db.replica.impl.circuitbreaker.BreakOnNotSupportedOperations;
import com.atlassian.db.replica.internal.ReadReplicaUnsupportedOperationException;
import com.atlassian.db.replica.internal.ReplicaCallableStatement;
import com.atlassian.db.replica.internal.ReplicaConnectionProvider;
import com.atlassian.db.replica.internal.ReplicaPreparedStatement;
import com.atlassian.db.replica.internal.ReplicaStatement;
import com.atlassian.db.replica.internal.circuitbreaker.BreakerConnection;
import com.atlassian.db.replica.internal.circuitbreaker.BreakerHandler;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.DualCall;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
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
        DualCall dualCall) {
        this.connectionProvider = new ReplicaConnectionProvider(connectionProvider, consistency);
        this.consistency = consistency;
        this.dualCall = dualCall;
    }

    @Override
    public Statement createStatement() {
        return ReplicaStatement.builder(connectionProvider, consistency, dualCall).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) {
        return new ReplicaPreparedStatement.Builder(connectionProvider, consistency, dualCall, sql).build();
    }

    @Override
    public CallableStatement prepareCall(String sql) {
        return new ReplicaCallableStatement.Builder(connectionProvider, consistency, dualCall, sql).build();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return connectionProvider.getWriteConnection().nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        connectionProvider.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() {
        return connectionProvider.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        connectionProvider.commit();
    }

    @Override
    public void rollback() throws SQLException {
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
        return connectionProvider.getWriteConnection().getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        connectionProvider.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() {
        return connectionProvider.getReadOnly();
    }

    @Override
    public void setCatalog(String catalog) {
        connectionProvider.setCatalog(catalog);
    }

    @Override
    public String getCatalog() {
        return connectionProvider.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) {
        connectionProvider.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        //noinspection MagicConstant
        return connectionProvider.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connectionProvider.getWarning();
    }

    @Override
    public void clearWarnings() throws SQLException {
        connectionProvider.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) {
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
    ) {
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
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
        return new ReplicaCallableStatement
            .Builder(connectionProvider, consistency, dualCall, sql)
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return connectionProvider.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
        connectionProvider.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) {
        connectionProvider.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return connectionProvider.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) {
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
    ) {
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
    ) {
        return new ReplicaCallableStatement
            .Builder(connectionProvider, consistency, dualCall, sql)
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .resultSetHoldability(resultSetHoldability)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            dualCall,
            sql
        ).autoGeneratedKeys(autoGeneratedKeys)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            dualCall,
            sql
        ).columnIndexes(columnIndexes)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) {
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            dualCall,
            sql
        ).columnNames(columnNames)
            .build();
    }

    @Override
    public Clob createClob() {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Blob createBlob() {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public NClob createNClob() {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return connectionProvider.getReadConnection().isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setClientInfo(Properties properties) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Properties getClientInfo() {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return connectionProvider.getWriteConnection().createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public String getSchema() throws SQLException {
        return connectionProvider.getReadConnection().getSchema();
    }

    @Override
    public void abort(Executor executor) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeout() {
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
}
