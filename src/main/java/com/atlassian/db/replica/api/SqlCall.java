package com.atlassian.db.replica.api;

import java.sql.*;

public interface SqlCall<T> {
    T call() throws SQLException;
}
