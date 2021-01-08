package com.atlassian.db.replica.api;

import java.sql.*;

/**
 * Like a {@link java.util.concurrent.Callable}, but with a checked exception.
 * @param <T>
 */
public interface SqlCall<T> {
    T call() throws SQLException;
}
