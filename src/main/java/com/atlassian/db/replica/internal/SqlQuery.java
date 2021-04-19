package com.atlassian.db.replica.internal;

public interface SqlQuery {
    static SqlQuery create(String sql, boolean compatibleWithPreviousVersion){
        if(compatibleWithPreviousVersion){
            return new SqlQueryOld(sql);
        }else{
            return new SqlQueryNew(sql);
        }
    }

    boolean isSqlSet();
    boolean isWriteOperation(SqlFunction sqlFunction);
    boolean isSelectForUpdate();

}
