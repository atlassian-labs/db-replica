package com.atlassian.db.replica.internal.circuitbreaker;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class BreakerCallableStatement extends BreakerPreparedStatement implements CallableStatement {
    private final CallableStatement delegate;
    private final BreakerHandler breakerHandler;

    public BreakerCallableStatement(CallableStatement delegate, BreakerHandler breakerHandler) {
        super(delegate, breakerHandler);
        this.delegate = delegate;
        this.breakerHandler = breakerHandler;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterIndex, sqlType));
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterIndex, sqlType, scale));
    }

    @Override
    public boolean wasNull() throws SQLException {
        return breakerHandler.handle(delegate::wasNull);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getString(parameterIndex));
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBoolean(parameterIndex));
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getByte(parameterIndex));
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getShort(parameterIndex));
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getInt(parameterIndex));
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getLong(parameterIndex));
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getFloat(parameterIndex));
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getDouble(parameterIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBigDecimal(parameterIndex));
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBytes(parameterIndex));
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getDate(parameterIndex));
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTime(parameterIndex));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTimestamp(parameterIndex));
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getObject(parameterIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBigDecimal(parameterIndex));
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return breakerHandler.handle(() -> delegate.getObject(parameterIndex, map));
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getRef(parameterIndex));
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBlob(parameterIndex));
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getClob(parameterIndex));
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getArray(parameterIndex));
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return breakerHandler.handle(() -> delegate.getDate(parameterIndex, cal));
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTime(parameterIndex, cal));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTimestamp(parameterIndex, cal));
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterIndex, sqlType, typeName));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterName, sqlType));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterName, sqlType, scale));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterName, sqlType, typeName));
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getURL(parameterIndex));
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        breakerHandler.handle(() -> delegate.setURL(parameterName, val));
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        breakerHandler.handle(() -> delegate.setNull(parameterName, sqlType));
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        breakerHandler.handle(() -> delegate.setBoolean(parameterName, x));
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        breakerHandler.handle(() -> delegate.setByte(parameterName, x));
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        breakerHandler.handle(() -> delegate.setShort(parameterName, x));
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        breakerHandler.handle(() -> delegate.setInt(parameterName, x));
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        breakerHandler.handle(() -> delegate.setLong(parameterName, x));
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        breakerHandler.handle(() -> delegate.setFloat(parameterName, x));
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        breakerHandler.handle(() -> delegate.setDouble(parameterName, x));
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        breakerHandler.handle(() -> delegate.setBigDecimal(parameterName, x));
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        breakerHandler.handle(() -> delegate.setString(parameterName, x));
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        breakerHandler.handle(() -> delegate.setBytes(parameterName, x));
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        breakerHandler.handle(() -> delegate.setDate(parameterName, x));
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        breakerHandler.handle(() -> delegate.setTime(parameterName, x));
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        breakerHandler.handle(() -> delegate.setTimestamp(parameterName, x));
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        breakerHandler.handle(() -> delegate.setAsciiStream(parameterName, x, length));
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        breakerHandler.handle(() -> delegate.setBinaryStream(parameterName, x, length));
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        breakerHandler.handle(() -> delegate.setObject(parameterName, x, targetSqlType, scale));
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        breakerHandler.handle(() -> delegate.setObject(parameterName, x, targetSqlType));
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        breakerHandler.handle(() -> delegate.setObject(parameterName, x));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        breakerHandler.handle(() -> delegate.setCharacterStream(parameterName, reader, length));
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        breakerHandler.handle(() -> delegate.setDate(parameterName, x, cal));
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        breakerHandler.handle(() -> delegate.setTime(parameterName, x, cal));
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        breakerHandler.handle(() -> delegate.setTimestamp(parameterName, x, cal));
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        breakerHandler.handle(() -> delegate.setNull(parameterName, sqlType, typeName));
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getString(parameterName));
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBoolean(parameterName));
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getByte(parameterName));
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getShort(parameterName));
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getInt(parameterName));
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getLong(parameterName));
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getFloat(parameterName));
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getDouble(parameterName));
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBytes(parameterName));
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getDate(parameterName));
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTime(parameterName));
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTimestamp(parameterName));
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getObject(parameterName));
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBigDecimal(parameterName));
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return breakerHandler.handle(() -> delegate.getObject(parameterName, map));
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getRef(parameterName));
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getBlob(parameterName));
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getClob(parameterName));
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getArray(parameterName));
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return breakerHandler.handle(() -> delegate.getDate(parameterName, cal));
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTime(parameterName, cal));
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return breakerHandler.handle(() -> delegate.getTimestamp(parameterName, cal));
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getURL(parameterName));
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getRowId(parameterIndex));
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getRowId(parameterName));
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        breakerHandler.handle(() -> delegate.setRowId(parameterName, x));
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        breakerHandler.handle(() -> delegate.setNString(parameterName, value));
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        breakerHandler.handle(() -> delegate.setNCharacterStream(parameterName, value, length));
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        breakerHandler.handle(() -> delegate.setNClob(parameterName, value));
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        breakerHandler.handle(() -> delegate.setClob(parameterName, reader, length));
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        breakerHandler.handle(() -> delegate.setBlob(parameterName, inputStream, length));
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        breakerHandler.handle(() -> delegate.setNClob(parameterName, reader, length));
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getNClob(parameterIndex));
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getNClob(parameterName));
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        breakerHandler.handle(() -> delegate.setSQLXML(parameterName, xmlObject));
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getSQLXML(parameterIndex));
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getSQLXML(parameterName));
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getNString(parameterIndex));
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getNString(parameterName));
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getNCharacterStream(parameterIndex));
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getNCharacterStream(parameterName));
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return breakerHandler.handle(() -> delegate.getCharacterStream(parameterIndex));
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return breakerHandler.handle(() -> delegate.getCharacterStream(parameterName));
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        breakerHandler.handle(() -> delegate.setBlob(parameterName, x));
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        breakerHandler.handle(() -> delegate.setClob(parameterName, x));
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        breakerHandler.handle(() -> delegate.setAsciiStream(parameterName, x, length));
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        breakerHandler.handle(() -> delegate.setBinaryStream(parameterName, x, length));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        breakerHandler.handle(() -> delegate.setCharacterStream(parameterName, reader, length));
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        breakerHandler.handle(() -> delegate.setAsciiStream(parameterName, x));
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        breakerHandler.handle(() -> delegate.setBinaryStream(parameterName, x));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        breakerHandler.handle(() -> delegate.setCharacterStream(parameterName, reader));
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        breakerHandler.handle(() -> delegate.setNCharacterStream(parameterName, value));
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        breakerHandler.handle(() -> delegate.setClob(parameterName, reader));
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        breakerHandler.handle(() -> delegate.setBlob(parameterName, inputStream));
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        breakerHandler.handle(() -> delegate.setNClob(parameterName, reader));
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return breakerHandler.handle(() -> delegate.getObject(parameterIndex, type));
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return breakerHandler.handle(() -> delegate.getObject(parameterName, type));
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType,
                          int scaleOrLength) throws SQLException {
        breakerHandler.handle(() -> delegate.setObject(parameterName, x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        breakerHandler.handle(() -> delegate.setObject(parameterName, x, targetSqlType));
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterIndex, sqlType));
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType,
                                     int scale) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterIndex, sqlType, scale));
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType,
                                     String typeName) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterIndex, sqlType, typeName));
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterName, sqlType));
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType,
                                     int scale) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterName, sqlType, scale));
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType,
                                     String typeName) throws SQLException {
        breakerHandler.handle(() -> delegate.registerOutParameter(parameterName, sqlType, typeName));
    }
}
