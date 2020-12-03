package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;

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

public class BreakerConnection implements Connection {
    private final Connection delegate;
    private final BreakerHandler breakerHandler;

    public BreakerConnection(Connection delegate, BreakerHandler breakerHandler) {
        this.delegate = delegate;
        this.breakerHandler = breakerHandler;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new BreakerStatement(
            breakerHandler.handle((SqlCall<Statement>) delegate::createStatement),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql)),
            breakerHandler
        );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new BreakerCallableStatement(breakerHandler.handle(() -> delegate.prepareCall(sql)), breakerHandler);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return breakerHandler.handle(() -> delegate.nativeSQL(sql));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        breakerHandler.handle(() -> delegate.setAutoCommit(autoCommit));
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return breakerHandler.handle(delegate::getAutoCommit);
    }

    @Override
    public void commit() throws SQLException {
        breakerHandler.handle(delegate::commit);
    }

    @Override
    public void rollback() throws SQLException {
        breakerHandler.handle((BreakerHandler.SqlRunnable) delegate::rollback);
    }

    @Override
    public void close() throws SQLException {
        breakerHandler.handle(delegate::close);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return breakerHandler.handle(delegate::isClosed);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return breakerHandler.handle(delegate::getMetaData);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        breakerHandler.handle(() -> delegate.setReadOnly(readOnly));
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return breakerHandler.handle(delegate::isReadOnly);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        breakerHandler.handle(() -> delegate.setCatalog(catalog));
    }

    @Override
    public String getCatalog() throws SQLException {
        return breakerHandler.handle(delegate::getCatalog);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        breakerHandler.handle(() -> delegate.setTransactionIsolation(level));
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        //noinspection MagicConstant
        return breakerHandler.handle(delegate::getTransactionIsolation);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return breakerHandler.handle(delegate::getWarnings);
    }

    @Override
    public void clearWarnings() throws SQLException {
        breakerHandler.handle(delegate::clearWarnings);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new BreakerStatement(breakerHandler.handle(() -> delegate.createStatement(
            resultSetType,
            resultSetConcurrency
        )), breakerHandler);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency
    ) throws SQLException {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, resultSetType, resultSetConcurrency)),
            breakerHandler
        );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new BreakerCallableStatement(
            breakerHandler.handle(() -> delegate.prepareCall(sql, resultSetType, resultSetConcurrency)),
            breakerHandler
        );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return breakerHandler.handle(delegate::getTypeMap);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        breakerHandler.handle(() -> delegate.setTypeMap(map));
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        breakerHandler.handle(() -> delegate.setHoldability(holdability));
    }

    @Override
    public int getHoldability() throws SQLException {
        return breakerHandler.handle(delegate::getHoldability);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return breakerHandler.handle((SqlCall<Savepoint>) delegate::setSavepoint);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return breakerHandler.handle(() -> delegate.setSavepoint(name));
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        breakerHandler.handle(() -> delegate.rollback(savepoint));
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        breakerHandler.handle(() -> delegate.releaseSavepoint(savepoint));
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return new BreakerStatement(
            breakerHandler.handle(() -> delegate.createStatement(
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            breakerHandler
        );
    }

    @Override
    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return new BreakerCallableStatement(
            breakerHandler.handle(() -> delegate.prepareCall(
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, autoGeneratedKeys)),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, columnIndexes)),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, columnNames)),
            breakerHandler
        );
    }

    @Override
    public Clob createClob() throws SQLException {
        return breakerHandler.handle(delegate::createClob);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return breakerHandler.handle(delegate::createBlob);
    }

    @Override
    public NClob createNClob() throws SQLException {
        return breakerHandler.handle(delegate::createNClob);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return breakerHandler.handle(delegate::createSQLXML);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return breakerHandler.handle(() -> delegate.isValid(timeout));
    }

    @Override
    public void setClientInfo(String name, String value) {
        try {
            breakerHandler.handle(() -> delegate.setClientInfo(name, value));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public void setClientInfo(Properties properties) {
        try {
            breakerHandler.handle(() -> delegate.setClientInfo(properties));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return breakerHandler.handle(() -> delegate.getClientInfo(name));
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return breakerHandler.handle((SqlCall<Properties>) delegate::getClientInfo);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return breakerHandler.handle(() -> delegate.createArrayOf(typeName, elements));
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return breakerHandler.handle(() -> delegate.createStruct(typeName, attributes));
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        breakerHandler.handle(() -> delegate.setSchema(schema));
    }

    @Override
    public String getSchema() throws SQLException {
        return breakerHandler.handle(delegate::getSchema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        breakerHandler.handle(() -> delegate.abort(executor));
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        breakerHandler.handle(() -> delegate.setNetworkTimeout(executor, milliseconds));
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return breakerHandler.handle(delegate::getNetworkTimeout);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return breakerHandler.handle(() -> delegate.unwrap(iface));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return breakerHandler.handle(() -> delegate.isWrapperFor(iface));
    }
}
