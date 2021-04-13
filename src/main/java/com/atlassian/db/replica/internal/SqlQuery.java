package com.atlassian.db.replica.internal;

public final class SqlQuery {
    private static final int SELECT_FOR_UPDATE_SUFFIX_LIMIT = 100;

    private final String sql;
    private final boolean compatibleWithPreviousVersion;

    public SqlQuery(String sql, boolean compatibleWithPreviousVersion) {
        this.sql = sql;
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
    }

    boolean isWriteOperation(SqlFunction sqlFunction) {
        return sqlFunction.isFunctionCall(sql) || isUpdate() || isDelete();
    }

    boolean isSelectForUpdate() {
        if (compatibleWithPreviousVersion) {
            return isSelectForUpdateOld();
        } else {
            return isSelectForUpdateNew();
        }
    }

    private boolean isSelectForUpdateNew() {
        final String trimmedQuery = trimForSelectForUpdateCheck();
        return sql != null && (trimmedQuery.contains("for update") || trimmedQuery.contains("FOR UPDATE"));
    }

    private boolean isSelectForUpdateOld() {
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

    private String trimForSelectForUpdateCheck() {
        if (sql.length() < SELECT_FOR_UPDATE_SUFFIX_LIMIT) {
            return sql;
        } else {
            return sql.substring(sql.length() - SELECT_FOR_UPDATE_SUFFIX_LIMIT);
        }
    }
}
