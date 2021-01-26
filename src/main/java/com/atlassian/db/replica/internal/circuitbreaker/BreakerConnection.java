package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;

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
    private final CircuitBreaker breaker;

    public BreakerConnection(Connection delegate, CircuitBreaker breaker) {
        this.delegate = delegate;
        this.breaker = breaker;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new BreakerStatement(
            breaker.handle((SqlCall<Statement>) delegate::createStatement),
            breaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new BreakerPreparedStatement(
            breaker.handle(() -> delegate.prepareStatement(sql)),
            breaker
        );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new BreakerCallableStatement(breaker.handle(() -> delegate.prepareCall(sql)), breaker);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return breaker.handle(() -> delegate.nativeSQL(sql));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        breaker.handle(() -> delegate.setAutoCommit(autoCommit));
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return breaker.handle(delegate::getAutoCommit);
    }

    @Override
    public void commit() throws SQLException {
        breaker.handle(delegate::commit);
    }

    @Override
    public void rollback() throws SQLException {
        breaker.handle((CircuitBreaker.SqlRunnable) delegate::rollback);
    }

    @Override
    public void close() throws SQLException {
        breaker.handle(delegate::close);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return breaker.handle(delegate::isClosed);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return breaker.handle(delegate::getMetaData);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        breaker.handle(() -> delegate.setReadOnly(readOnly));
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return breaker.handle(delegate::isReadOnly);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        breaker.handle(() -> delegate.setCatalog(catalog));
    }

    @Override
    public String getCatalog() throws SQLException {
        return breaker.handle(delegate::getCatalog);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        breaker.handle(() -> delegate.setTransactionIsolation(level));
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        //noinspection MagicConstant
        return breaker.handle(delegate::getTransactionIsolation);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return breaker.handle(delegate::getWarnings);
    }

    @Override
    public void clearWarnings() throws SQLException {
        breaker.handle(delegate::clearWarnings);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new BreakerStatement(breaker.handle(() -> delegate.createStatement(
            resultSetType,
            resultSetConcurrency
        )), breaker);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency
    ) throws SQLException {
        return new BreakerPreparedStatement(
            breaker.handle(() -> delegate.prepareStatement(sql, resultSetType, resultSetConcurrency)),
            breaker
        );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new BreakerCallableStatement(
            breaker.handle(() -> delegate.prepareCall(sql, resultSetType, resultSetConcurrency)),
            breaker
        );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return breaker.handle(delegate::getTypeMap);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        breaker.handle(() -> delegate.setTypeMap(map));
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        breaker.handle(() -> delegate.setHoldability(holdability));
    }

    @Override
    public int getHoldability() throws SQLException {
        return breaker.handle(delegate::getHoldability);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return breaker.handle((SqlCall<Savepoint>) delegate::setSavepoint);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return breaker.handle(() -> delegate.setSavepoint(name));
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        breaker.handle(() -> delegate.rollback(savepoint));
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        breaker.handle(() -> delegate.releaseSavepoint(savepoint));
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return new BreakerStatement(
            breaker.handle(() -> delegate.createStatement(
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            breaker
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
            breaker.handle(() -> delegate.prepareStatement(
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            breaker
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
            breaker.handle(() -> delegate.prepareCall(
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            breaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new BreakerPreparedStatement(
            breaker.handle(() -> delegate.prepareStatement(sql, autoGeneratedKeys)),
            breaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new BreakerPreparedStatement(
            breaker.handle(() -> delegate.prepareStatement(sql, columnIndexes)),
            breaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new BreakerPreparedStatement(
            breaker.handle(() -> delegate.prepareStatement(sql, columnNames)),
            breaker
        );
    }

    @Override
    public Clob createClob() throws SQLException {
        return breaker.handle(delegate::createClob);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return breaker.handle(delegate::createBlob);
    }

    @Override
    public NClob createNClob() throws SQLException {
        return breaker.handle(delegate::createNClob);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return breaker.handle(delegate::createSQLXML);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return breaker.handle(() -> delegate.isValid(timeout));
    }

    @Override
    public void setClientInfo(String name, String value) {
        try {
            breaker.handle(() -> delegate.setClientInfo(name, value));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public void setClientInfo(Properties properties) {
        try {
            breaker.handle(() -> delegate.setClientInfo(properties));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return breaker.handle(() -> delegate.getClientInfo(name));
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return breaker.handle((SqlCall<Properties>) delegate::getClientInfo);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return breaker.handle(() -> delegate.createArrayOf(typeName, elements));
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return breaker.handle(() -> delegate.createStruct(typeName, attributes));
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        breaker.handle(() -> delegate.setSchema(schema));
    }

    @Override
    public String getSchema() throws SQLException {
        return breaker.handle(delegate::getSchema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        breaker.handle(() -> delegate.abort(executor));
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        breaker.handle(() -> delegate.setNetworkTimeout(executor, milliseconds));
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return breaker.handle(delegate::getNetworkTimeout);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return breaker.handle(() -> delegate.unwrap(iface));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return breaker.handle(() -> delegate.isWrapperFor(iface));
    }
}
