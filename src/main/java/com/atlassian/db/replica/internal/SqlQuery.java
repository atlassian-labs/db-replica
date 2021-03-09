package com.atlassian.db.replica.internal;

public final class SqlQuery {

    private final String sql;

    public SqlQuery(String sql) {
        this.sql = sql;
    }

    boolean isWriteOperation(SqlFunction sqlFunction) {
        return sqlFunction.isFunctionCall(sql) || isUpdate() || isDelete();
    }

    boolean isSelectForUpdate() {
        return sql != null && (sql.endsWith("for update") || sql.endsWith("FOR UPDATE"));
    }

    boolean isSqlSet() {
        return sql.startsWith("set");
    }

    private boolean isUpdate() {
        return sql != null && (sql.startsWith("update") || sql.startsWith("UPDATE"));
    }

    private boolean isDelete() {
        return sql != null && (sql.startsWith("delete") || sql.startsWith("DELETE"));
    }
}
