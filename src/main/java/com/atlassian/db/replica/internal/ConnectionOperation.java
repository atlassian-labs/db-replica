package com.atlassian.db.replica.internal;


import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionOperation {
    void accept(Connection connection) throws SQLException;
}
