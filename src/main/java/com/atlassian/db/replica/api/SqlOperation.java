package com.atlassian.db.replica.api;

import java.sql.*;

public interface SqlOperation<T> {
    T execute() throws SQLException;
}
