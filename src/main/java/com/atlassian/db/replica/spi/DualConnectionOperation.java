package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.SqlOperation;

import java.sql.SQLException;

/**
 * Wraps calls to database. Can be used to collect metrics.
 */
public interface DualConnectionOperation {
    <T> T executeOnReplica(final SqlOperation<T> operation) throws SQLException;

    <T> T executeOnMain(final SqlOperation<T> operation) throws SQLException;
}
