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
    public Statement createStatement() {
        return new BreakerStatement(breakerHandler.handle((SqlCall<Statement>) delegate::createStatement), breakerHandler);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) {
        return new BreakerPreparedStatement(breakerHandler.handle(() -> delegate.prepareStatement(sql)), breakerHandler);
    }

    @Override
    public CallableStatement prepareCall(String sql) {
        return new BreakerCallableStatement(breakerHandler.handle(() -> delegate.prepareCall(sql)), breakerHandler);
    }

    @Override
    public String nativeSQL(String sql) {
        return breakerHandler.handle(() -> delegate.nativeSQL(sql));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        breakerHandler.handle(() -> delegate.setAutoCommit(autoCommit));
    }

    @Override
    public boolean getAutoCommit() {
        return breakerHandler.handle(delegate::getAutoCommit);
    }

    @Override
    public void commit() {
        breakerHandler.handle(delegate::commit);
    }

    @Override
    public void rollback() {
        breakerHandler.handle((BreakerHandler.SqlRunnable) delegate::rollback);
    }

    @Override
    public void close() {
        breakerHandler.handle(delegate::close);
    }

    @Override
    public boolean isClosed() {
        return breakerHandler.handle(delegate::isClosed);
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return breakerHandler.handle(delegate::getMetaData);
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        breakerHandler.handle(() -> delegate.setReadOnly(readOnly));
    }

    @Override
    public boolean isReadOnly() {
        return breakerHandler.handle(delegate::isReadOnly);
    }

    @Override
    public void setCatalog(String catalog) {
        breakerHandler.handle(() -> delegate.setCatalog(catalog));
    }

    @Override
    public String getCatalog() {
        return breakerHandler.handle(delegate::getCatalog);
    }

    @Override
    public void setTransactionIsolation(int level) {
        breakerHandler.handle(() -> delegate.setTransactionIsolation(level));
    }

    @Override
    public int getTransactionIsolation() {
        //noinspection MagicConstant
        return breakerHandler.handle(delegate::getTransactionIsolation);
    }

    @Override
    public SQLWarning getWarnings() {
        return breakerHandler.handle(delegate::getWarnings);
    }

    @Override
    public void clearWarnings() {
        breakerHandler.handle(delegate::clearWarnings);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) {
        return new BreakerStatement(breakerHandler.handle(() -> delegate.createStatement(resultSetType, resultSetConcurrency)), breakerHandler);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, resultSetType, resultSetConcurrency)),
            breakerHandler
        );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
        return new BreakerCallableStatement(
            breakerHandler.handle(() -> delegate.prepareCall(sql, resultSetType, resultSetConcurrency)),
            breakerHandler
        );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return breakerHandler.handle(delegate::getTypeMap);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
        breakerHandler.handle(() -> delegate.setTypeMap(map));
    }

    @Override
    public void setHoldability(int holdability) {
        breakerHandler.handle(() -> delegate.setHoldability(holdability));
    }

    @Override
    public int getHoldability() {
        return breakerHandler.handle(delegate::getHoldability);
    }

    @Override
    public Savepoint setSavepoint() {
        return breakerHandler.handle((SqlCall<Savepoint>) delegate::setSavepoint);
    }

    @Override
    public Savepoint setSavepoint(String name) {
        return breakerHandler.handle(() -> delegate.setSavepoint(name));
    }

    @Override
    public void rollback(Savepoint savepoint) {
        breakerHandler.handle(() -> delegate.rollback(savepoint));
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) {
        breakerHandler.handle(() -> delegate.releaseSavepoint(savepoint));
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return new BreakerStatement(
            breakerHandler.handle(() -> delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)),
            breakerHandler
        );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return new BreakerCallableStatement(
            breakerHandler.handle(() -> delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, autoGeneratedKeys)),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, columnIndexes)),
            breakerHandler
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) {
        return new BreakerPreparedStatement(
            breakerHandler.handle(() -> delegate.prepareStatement(sql, columnNames)),
            breakerHandler
        );
    }

    @Override
    public Clob createClob() {
        return breakerHandler.handle(delegate::createClob);
    }

    @Override
    public Blob createBlob() {
        return breakerHandler.handle(delegate::createBlob);
    }

    @Override
    public NClob createNClob() {
        return breakerHandler.handle(delegate::createNClob);
    }

    @Override
    public SQLXML createSQLXML() {
        return breakerHandler.handle(delegate::createSQLXML);
    }

    @Override
    public boolean isValid(int timeout) {
        return breakerHandler.handle(() -> delegate.isValid(timeout));
    }

    @Override
    public void setClientInfo(String name, String value) {
        breakerHandler.handle(() -> delegate.setClientInfo(name, value));
    }

    @Override
    public void setClientInfo(Properties properties) {
        breakerHandler.handle(() -> delegate.setClientInfo(properties));
    }

    @Override
    public String getClientInfo(String name) {
        return breakerHandler.handle(() -> delegate.getClientInfo(name));
    }

    @Override
    public Properties getClientInfo() {
        return breakerHandler.handle((SqlCall<Properties>) delegate::getClientInfo);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) {
        return breakerHandler.handle(() -> delegate.createArrayOf(typeName, elements));
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) {
        return breakerHandler.handle(() -> delegate.createStruct(typeName, attributes));
    }

    @Override
    public void setSchema(String schema) {
        breakerHandler.handle(() -> delegate.setSchema(schema));
    }

    @Override
    public String getSchema() {
        return breakerHandler.handle(delegate::getSchema);
    }

    @Override
    public void abort(Executor executor) {
        breakerHandler.handle(() -> delegate.abort(executor));
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        breakerHandler.handle(() -> delegate.setNetworkTimeout(executor, milliseconds));
    }

    @Override
    public int getNetworkTimeout() {
        return breakerHandler.handle(delegate::getNetworkTimeout);
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return breakerHandler.handle(() -> delegate.unwrap(iface));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return breakerHandler.handle(() -> delegate.isWrapperFor(iface));
    }
}
