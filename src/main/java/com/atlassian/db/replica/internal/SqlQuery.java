package com.atlassian.db.replica.internal;

public final class SqlQuery {
    private static final int SELECT_FOR_UPDATE_SUFFIX_LIMIT = 100;

    private final String sql;

    public SqlQuery(String sql) {
        if (sql == null) {
            throw new RuntimeException("An SqlQuery must have an SQL query string");
        }
        this.sql = sql;
    }

    public boolean isWriteOperation(SqlFunction sqlFunction) {
        return sqlFunction.isFunctionCall(sql) || isUpdate() || isDelete();
    }

    public boolean isSelectForUpdate() {
        final String trimmedQuery = trimForSelectForUpdateCheck();
        return trimmedQuery.contains("for update") || trimmedQuery.contains("FOR UPDATE");
    }

    public boolean isSqlSet() {
        return sql.startsWith("set");
    }

    private boolean isUpdate() {
        return sql.startsWith("update") || sql.startsWith("UPDATE");
    }

    private boolean isDelete() {
        return sql.startsWith("delete") || sql.startsWith("DELETE");
    }

    private String trimForSelectForUpdateCheck() {
        if (sql.length() < SELECT_FOR_UPDATE_SUFFIX_LIMIT) {
            return sql;
        } else {
            return sql.substring(sql.length() - SELECT_FOR_UPDATE_SUFFIX_LIMIT);
        }
    }
}
