package com.atlassian.db.replica.internal.connection.statements.operations;


import java.sql.SQLException;
import java.sql.Statement;

public interface Operation<T extends Statement> {
    void accept(T t) throws SQLException;
}
