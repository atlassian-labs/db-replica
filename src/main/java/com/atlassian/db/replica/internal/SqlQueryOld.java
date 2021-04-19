package com.atlassian.db.replica.internal;

public final class SqlQueryOld implements SqlQuery {
    private static final int SELECT_FOR_UPDATE_SUFFIX_LIMIT = 100;

    private final String sql;

    SqlQueryOld(String sql) {
        this.sql = sql;
    }

    @Override
    public boolean isWriteOperation(SqlFunction sqlFunction) {
        return sqlFunction.isFunctionCall(sql) || isUpdate() || isDelete();
    }

    public boolean isSelectForUpdate() {
        final String trimmedQuery = trimForSelectForUpdateCheck();
        return sql != null && (trimmedQuery.contains("for update") || trimmedQuery.contains("FOR UPDATE"));
    }

    @Override
    public boolean isSqlSet() {
        return sql.startsWith("set");
    }

    private boolean isUpdate() {
        return sql != null && (sql.startsWith("update") || sql.startsWith("UPDATE"));
    }

    private boolean isDelete() {
        return sql != null && (sql.startsWith("delete") || sql.startsWith("DELETE"));
    }

    private String trimForSelectForUpdateCheck() {
        if (sql.length() < SELECT_FOR_UPDATE_SUFFIX_LIMIT) {
            return sql;
        } else {
            return sql.substring(sql.length() - SELECT_FOR_UPDATE_SUFFIX_LIMIT);
        }
    }
}
