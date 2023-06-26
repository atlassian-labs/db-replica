package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class BreakerStatement implements Statement {
    private final Statement delegate;
    private final BreakerHandler breakerHandler;

    public BreakerStatement(Statement delegate, BreakerHandler breakerHandler) {
        this.delegate = delegate;
        this.breakerHandler = breakerHandler;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeQuery(sql));
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeUpdate(sql));
    }

    @Override
    public void close() throws SQLException {
        breakerHandler.handle(delegate::close);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return breakerHandler.handle(delegate::getMaxFieldSize);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        breakerHandler.handle(() -> delegate.setMaxFieldSize(max));
    }

    @Override
    public int getMaxRows() throws SQLException {
        return breakerHandler.handle(delegate::getMaxRows);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        breakerHandler.handle(() -> delegate.setMaxRows(max));
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        breakerHandler.handle(() -> delegate.setEscapeProcessing(enable));
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return breakerHandler.handle(delegate::getQueryTimeout);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        breakerHandler.handle(() -> delegate.setQueryTimeout(seconds));
    }

    @Override
    public void cancel() throws SQLException {
        breakerHandler.handle(delegate::cancel);
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
    public void setCursorName(String name) throws SQLException {
        breakerHandler.handle(() -> delegate.setCursorName(name));
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return breakerHandler.handle(() -> delegate.execute(sql));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return breakerHandler.handle(delegate::getResultSet);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return breakerHandler.handle(delegate::getUpdateCount);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return breakerHandler.handle((SqlCall<Boolean>) delegate::getMoreResults);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        breakerHandler.handle(() -> delegate.setFetchDirection(direction));
    }

    @Override
    public int getFetchDirection() throws SQLException {
        //noinspection MagicConstant
        return breakerHandler.handle(delegate::getFetchDirection);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        breakerHandler.handle(() -> delegate.setFetchSize(rows));
    }

    @Override
    public int getFetchSize() throws SQLException {
        return breakerHandler.handle(delegate::getFetchSize);
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        //noinspection MagicConstant
        return breakerHandler.handle(delegate::getResultSetConcurrency);
    }

    @Override
    public int getResultSetType() throws SQLException {
        //noinspection MagicConstant
        return breakerHandler.handle(delegate::getResultSetType);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        breakerHandler.handle(() -> delegate.addBatch(sql));
    }

    @Override
    public void clearBatch() throws SQLException {
        breakerHandler.handle(delegate::clearBatch);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return breakerHandler.handle(delegate::executeBatch);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return breakerHandler.handle(delegate::getConnection);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return breakerHandler.handle(() -> delegate.getMoreResults(current));
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return breakerHandler.handle(delegate::getGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeUpdate(sql, autoGeneratedKeys));
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeUpdate(sql, columnIndexes));
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeUpdate(sql, columnNames));
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return breakerHandler.handle(() -> delegate.execute(sql, autoGeneratedKeys));
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return breakerHandler.handle(() -> delegate.execute(sql, columnIndexes));
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return breakerHandler.handle(() -> delegate.execute(sql, columnNames));
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return breakerHandler.handle(delegate::getResultSetHoldability);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return breakerHandler.handle(delegate::isClosed);
    }

    @Override
    public void setPoolable(@SuppressWarnings("SpellCheckingInspection") boolean poolable) throws SQLException {
        breakerHandler.handle(() -> delegate.setPoolable(poolable));
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return breakerHandler.handle(delegate::isPoolable);
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        breakerHandler.handle(delegate::closeOnCompletion);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return breakerHandler.handle(delegate::isCloseOnCompletion);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return breakerHandler.handle(() -> delegate.unwrap(iface));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return breakerHandler.handle(() -> delegate.isWrapperFor(iface));
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return breakerHandler.handle(delegate::getLargeUpdateCount);
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        breakerHandler.handle(() -> delegate.setLargeMaxRows(max));
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return breakerHandler.handle(delegate::getLargeMaxRows);
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return breakerHandler.handle(delegate::executeLargeBatch);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeLargeUpdate(sql));
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeLargeUpdate(sql, autoGeneratedKeys));
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeLargeUpdate(sql, columnIndexes));
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return breakerHandler.handle(() -> delegate.executeLargeUpdate(sql, columnNames));
    }
}
