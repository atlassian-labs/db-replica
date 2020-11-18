package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.SqlCall;

import java.sql.SQLException;

/**
 * Splits main/replica calls.
 */
public interface DualCall {
    <T> T callReplica(final SqlCall<T> call) throws SQLException;

    <T> T callMain(final SqlCall<T> call) throws SQLException;
}
