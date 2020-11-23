package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class BreakerPreparedStatement extends BreakerStatement implements PreparedStatement {
    private final PreparedStatement delegate;
    private final BreakerHandler breakerHandler;

    public BreakerPreparedStatement(PreparedStatement delegate, BreakerHandler breakerHandler) {
        super(delegate, breakerHandler);
        this.delegate = delegate;
        this.breakerHandler = breakerHandler;
    }

    @Override
    public ResultSet executeQuery() {
        return breakerHandler.handle((SqlCall<ResultSet>) delegate::executeQuery);
    }

    @Override
    public int executeUpdate() {
        return breakerHandler.handle((SqlCall<Integer>) delegate::executeUpdate);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) {
        breakerHandler.handle(() -> delegate.setNull(parameterIndex, sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        breakerHandler.handle(() -> delegate.setBoolean(parameterIndex, x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        breakerHandler.handle(() -> delegate.setByte(parameterIndex, x));
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        breakerHandler.handle(() -> delegate.setShort(parameterIndex, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        breakerHandler.handle(() -> delegate.setInt(parameterIndex, x));
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        breakerHandler.handle(() -> delegate.setLong(parameterIndex, x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        breakerHandler.handle(() -> delegate.setFloat(parameterIndex, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        breakerHandler.handle(() -> delegate.setDouble(parameterIndex, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        breakerHandler.handle(() -> delegate.setBigDecimal(parameterIndex, x));
    }

    @Override
    public void setString(int parameterIndex, String x) {
        breakerHandler.handle(() -> delegate.setString(parameterIndex, x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) {
        breakerHandler.handle(() -> delegate.setBytes(parameterIndex, x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        breakerHandler.handle(() -> delegate.setDate(parameterIndex, x));
    }

    @Override
    public void setTime(int parameterIndex, Time x) {
        breakerHandler.handle(() -> delegate.setTime(parameterIndex, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        breakerHandler.handle(() -> delegate.setTimestamp(parameterIndex, x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) {
        breakerHandler.handle(() -> delegate.setAsciiStream(parameterIndex, x, length));
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
        //noinspection deprecation
        breakerHandler.handle(() -> delegate.setUnicodeStream(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) {
        breakerHandler.handle(() -> delegate.setBinaryStream(parameterIndex, x, length));
    }

    @Override
    public void clearParameters() {
        breakerHandler.handle(delegate::clearParameters);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) {
        breakerHandler.handle(() -> delegate.setObject(parameterIndex, x, targetSqlType));
    }

    @Override
    public void setObject(int parameterIndex, Object x) {
        breakerHandler.handle(() -> delegate.setObject(parameterIndex, x));
    }

    @Override
    public boolean execute() {
        return breakerHandler.handle((SqlCall<Boolean>) delegate::execute);
    }

    @Override
    public void addBatch() {
        breakerHandler.handle((BreakerHandler.SqlRunnable) delegate::addBatch);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) {
        breakerHandler.handle(() -> delegate.setCharacterStream(parameterIndex, reader, length));
    }

    @Override
    public void setRef(int parameterIndex, Ref x) {
        breakerHandler.handle(() -> delegate.setRef(parameterIndex, x));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) {
        breakerHandler.handle(() -> delegate.setBlob(parameterIndex, x));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) {
        breakerHandler.handle(() -> delegate.setClob(parameterIndex, x));
    }

    @Override
    public void setArray(int parameterIndex, Array x) {
        breakerHandler.handle(() -> delegate.setArray(parameterIndex, x));
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return breakerHandler.handle(delegate::getMetaData);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) {
        breakerHandler.handle(() -> delegate.setDate(parameterIndex, x, cal));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) {
        breakerHandler.handle(() -> delegate.setTime(parameterIndex, x, cal));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
        breakerHandler.handle(() -> delegate.setTimestamp(parameterIndex, x, cal));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) {
        breakerHandler.handle(() -> delegate.setNull(parameterIndex, sqlType, typeName));
    }

    @Override
    public void setURL(int parameterIndex, URL x) {
        breakerHandler.handle(() -> delegate.setURL(parameterIndex, x));
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return breakerHandler.handle(delegate::getParameterMetaData);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) {
        breakerHandler.handle(() -> delegate.setRowId(parameterIndex, x));
    }

    @Override
    public void setNString(int parameterIndex, String value) {
        breakerHandler.handle(() -> delegate.setNString(parameterIndex, value));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) {
        breakerHandler.handle(() -> delegate.setNCharacterStream(parameterIndex, value, length));
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) {
        breakerHandler.handle(() -> delegate.setNClob(parameterIndex, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) {
        breakerHandler.handle(() -> delegate.setClob(parameterIndex, reader, length));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) {
        breakerHandler.handle(() -> delegate.setBlob(parameterIndex, inputStream, length));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) {
        breakerHandler.handle(() -> delegate.setNClob(parameterIndex, reader, length));
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
        breakerHandler.handle(() -> delegate.setSQLXML(parameterIndex, xmlObject));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
        breakerHandler.handle(() -> delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) {
        breakerHandler.handle(() -> delegate.setAsciiStream(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) {
        breakerHandler.handle(() -> delegate.setBinaryStream(parameterIndex, x, length));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) {
        breakerHandler.handle(() -> delegate.setCharacterStream(parameterIndex, reader, length));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) {
        breakerHandler.handle(() -> delegate.setAsciiStream(parameterIndex, x));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) {
        breakerHandler.handle(() -> delegate.setBinaryStream(parameterIndex, x));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) {
        breakerHandler.handle(() -> delegate.setCharacterStream(parameterIndex, reader));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) {
        breakerHandler.handle(() -> delegate.setNCharacterStream(parameterIndex, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) {
        breakerHandler.handle(() -> delegate.setClob(parameterIndex, reader));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) {
        breakerHandler.handle(() -> delegate.setBlob(parameterIndex, inputStream));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) {
        breakerHandler.handle(() -> delegate.setNClob(parameterIndex, reader));
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType,
                          int scaleOrLength) {
        breakerHandler.handle(() -> delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength));

    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) {
        breakerHandler.handle(() -> delegate.setObject(parameterIndex, x, targetSqlType));
    }

    @Override
    public long executeLargeUpdate() {
        return breakerHandler.handle((SqlCall<Long>) delegate::executeLargeUpdate);
    }
}
