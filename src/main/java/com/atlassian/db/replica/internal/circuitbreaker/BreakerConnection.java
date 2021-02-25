package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.CircuitBreaker;
import com.atlassian.db.replica.api.SqlRun;

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
    private final CircuitBreaker circuitBreaker;

    public BreakerConnection(Connection delegate, CircuitBreaker circuitBreaker) {
        this.delegate = delegate;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new BreakerStatement(
            circuitBreaker.handle((SqlCall<Statement>) delegate::createStatement),
            circuitBreaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new BreakerPreparedStatement(
            circuitBreaker.handle(() -> delegate.prepareStatement(sql)),
            circuitBreaker
        );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new BreakerCallableStatement(circuitBreaker.handle(() -> delegate.prepareCall(sql)), circuitBreaker);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return circuitBreaker.handle(() -> delegate.nativeSQL(sql));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        circuitBreaker.handle(() -> delegate.setAutoCommit(autoCommit));
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return circuitBreaker.handle(delegate::getAutoCommit);
    }

    @Override
    public void commit() throws SQLException {
        circuitBreaker.handle(delegate::commit);
    }

    @Override
    public void rollback() throws SQLException {
        circuitBreaker.handle((SqlRun) delegate::rollback);
    }

    @Override
    public void close() throws SQLException {
        circuitBreaker.handle(delegate::close);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return circuitBreaker.handle(delegate::isClosed);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return circuitBreaker.handle(delegate::getMetaData);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        circuitBreaker.handle(() -> delegate.setReadOnly(readOnly));
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return circuitBreaker.handle(delegate::isReadOnly);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        circuitBreaker.handle(() -> delegate.setCatalog(catalog));
    }

    @Override
    public String getCatalog() throws SQLException {
        return circuitBreaker.handle(delegate::getCatalog);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        circuitBreaker.handle(() -> delegate.setTransactionIsolation(level));
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        //noinspection MagicConstant
        return circuitBreaker.handle(delegate::getTransactionIsolation);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return circuitBreaker.handle(delegate::getWarnings);
    }

    @Override
    public void clearWarnings() throws SQLException {
        circuitBreaker.handle(delegate::clearWarnings);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new BreakerStatement(circuitBreaker.handle(() -> delegate.createStatement(
            resultSetType,
            resultSetConcurrency
        )), circuitBreaker);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency
    ) throws SQLException {
        return new BreakerPreparedStatement(
            circuitBreaker.handle(() -> delegate.prepareStatement(sql, resultSetType, resultSetConcurrency)),
            circuitBreaker
        );
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new BreakerCallableStatement(
            circuitBreaker.handle(() -> delegate.prepareCall(sql, resultSetType, resultSetConcurrency)),
            circuitBreaker
        );
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return circuitBreaker.handle(delegate::getTypeMap);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        circuitBreaker.handle(() -> delegate.setTypeMap(map));
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        circuitBreaker.handle(() -> delegate.setHoldability(holdability));
    }

    @Override
    public int getHoldability() throws SQLException {
        return circuitBreaker.handle(delegate::getHoldability);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return circuitBreaker.handle((SqlCall<Savepoint>) delegate::setSavepoint);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return circuitBreaker.handle(() -> delegate.setSavepoint(name));
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        circuitBreaker.handle(() -> delegate.rollback(savepoint));
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        circuitBreaker.handle(() -> delegate.releaseSavepoint(savepoint));
    }

    @Override
    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        return new BreakerStatement(
            circuitBreaker.handle(() -> delegate.createStatement(
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            circuitBreaker
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
            circuitBreaker.handle(() -> delegate.prepareStatement(
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            circuitBreaker
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
            circuitBreaker.handle(() -> delegate.prepareCall(
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability
            )),
            circuitBreaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new BreakerPreparedStatement(
            circuitBreaker.handle(() -> delegate.prepareStatement(sql, autoGeneratedKeys)),
            circuitBreaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new BreakerPreparedStatement(
            circuitBreaker.handle(() -> delegate.prepareStatement(sql, columnIndexes)),
            circuitBreaker
        );
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new BreakerPreparedStatement(
            circuitBreaker.handle(() -> delegate.prepareStatement(sql, columnNames)),
            circuitBreaker
        );
    }

    @Override
    public Clob createClob() throws SQLException {
        return circuitBreaker.handle(delegate::createClob);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return circuitBreaker.handle(delegate::createBlob);
    }

    @Override
    public NClob createNClob() throws SQLException {
        return circuitBreaker.handle(delegate::createNClob);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return circuitBreaker.handle(delegate::createSQLXML);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return circuitBreaker.handle(() -> delegate.isValid(timeout));
    }

    @Override
    public void setClientInfo(String name, String value) {
        try {
            circuitBreaker.handle(() -> delegate.setClientInfo(name, value));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public void setClientInfo(Properties properties) {
        try {
            circuitBreaker.handle(() -> delegate.setClientInfo(properties));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return circuitBreaker.handle(() -> delegate.getClientInfo(name));
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return circuitBreaker.handle((SqlCall<Properties>) delegate::getClientInfo);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return circuitBreaker.handle(() -> delegate.createArrayOf(typeName, elements));
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return circuitBreaker.handle(() -> delegate.createStruct(typeName, attributes));
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        circuitBreaker.handle(() -> delegate.setSchema(schema));
    }

    @Override
    public String getSchema() throws SQLException {
        return circuitBreaker.handle(delegate::getSchema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        circuitBreaker.handle(() -> delegate.abort(executor));
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        circuitBreaker.handle(() -> delegate.setNetworkTimeout(executor, milliseconds));
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return circuitBreaker.handle(delegate::getNetworkTimeout);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return circuitBreaker.handle(() -> delegate.unwrap(iface));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return circuitBreaker.handle(() -> delegate.isWrapperFor(iface));
    }
}
