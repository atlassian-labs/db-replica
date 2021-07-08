package com.atlassian.db.replica.api.mocks;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("RedundantThrows")
public class ConnectionMock implements Connection {
    private boolean isClosed = false;
    private boolean isAutoCommit = true;
    private boolean readOnly = false;

    @Override
    public Statement createStatement() throws SQLException {
        return mock(Statement.class);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.getConnection()).thenReturn(this);
        return preparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return mock(CallableStatement.class);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.isAutoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return isAutoCommit;
    }

    @Override
    public void commit() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void rollback() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed) {
            throw new SQLException("Connection closed");
        } else {
            return null;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return mock(Statement.class);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency
    ) throws SQLException {
        return mock(PreparedStatement.class);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return mock(CallableStatement.class);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return mock(Statement.class);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return mock(PreparedStatement.class);
    }

    @Override
    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return mock(CallableStatement.class);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return mock(PreparedStatement.class);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return mock(PreparedStatement.class);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return mock(PreparedStatement.class);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new RuntimeException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new RuntimeException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new RuntimeException();
    }
}
