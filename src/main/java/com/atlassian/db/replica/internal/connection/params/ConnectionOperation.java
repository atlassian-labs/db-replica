package com.atlassian.db.replica.internal.connection.params;


import java.sql.Connection;
import java.sql.SQLException;

interface ConnectionOperation {
    void accept(Connection connection) throws SQLException;
}
