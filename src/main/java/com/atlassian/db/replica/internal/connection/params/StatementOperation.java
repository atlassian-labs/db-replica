package com.atlassian.db.replica.internal.connection.params;


import java.sql.SQLException;
import java.sql.Statement;

public interface StatementOperation<T extends Statement> {
    void accept(T t) throws SQLException;
}
