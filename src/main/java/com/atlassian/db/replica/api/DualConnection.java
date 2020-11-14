package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.*;
import com.atlassian.db.replica.spi.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Uses main database connections for UPDATE, INSERT and delete queries.
 * Uses replica connection for SELECT queries if the replica is in sync with the main database.
 */
public class DualConnection implements Connection {
    private final ReplicaConnectionProvider connectionProvider;
    private final ReplicaConsistency consistency;

    public DualConnection(ConnectionProvider connectionProvider, ReplicaConsistency consistency) {
        this.connectionProvider = new ReplicaConnectionProvider(connectionProvider, consistency);
        this.consistency = consistency;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return ReplicaStatement.builder(connectionProvider, consistency).build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new ReplicaPreparedStatement.Builder(connectionProvider, consistency, sql).build();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new ReplicaCallableStatement.Builder(connectionProvider, consistency, sql).build();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return connectionProvider.getWriteConnection().nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        connectionProvider.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
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
    public boolean isClosed() throws SQLException {
        return connectionProvider.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return connectionProvider.getWriteConnection().getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        connectionProvider.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return ReplicaStatement
            .builder(connectionProvider, consistency)
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
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            sql
        ).resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new ReplicaCallableStatement
            .Builder(connectionProvider, consistency, sql)
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .build();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return ReplicaStatement.builder(connectionProvider, consistency)
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
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
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
        return new ReplicaCallableStatement
            .Builder(connectionProvider, consistency, sql)
            .resultSetType(resultSetType)
            .resultSetConcurrency(resultSetConcurrency)
            .resultSetHoldability(resultSetHoldability)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            sql
        ).autoGeneratedKeys(autoGeneratedKeys)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            sql
        ).columnIndexes(columnIndexes)
            .build();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new ReplicaPreparedStatement.Builder(
            connectionProvider,
            consistency,
            sql
        ).columnNames(columnNames)
            .build();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new ReadReplicaUnsupportedOperationException();
    }
}
