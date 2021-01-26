package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.internal.ReadReplicaUnsupportedOperationException;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class BreakOnNotSupportedOperations implements CircuitBreaker {

    private static volatile boolean ALL_CALLS_SUPPORTED_SO_FAR = true;

    @Override
    public boolean canCall() {
        return ALL_CALLS_SUPPORTED_SO_FAR;
    }

    @Override
    public <T> T handle(SqlCall<T> call) throws SQLException {
        try {
            return call.call();
        } catch (ReadReplicaUnsupportedOperationException | SQLFeatureNotSupportedException e) {
            ALL_CALLS_SUPPORTED_SO_FAR = false;
            throw e;
        }
    }

    @Override
    public void handle(SqlRunnable runnable) throws SQLException {
        try {
            runnable.run();
        } catch (ReadReplicaUnsupportedOperationException | SQLFeatureNotSupportedException e) {
            ALL_CALLS_SUPPORTED_SO_FAR = false;
            throw e;
        }
    }

    public static void reset() {
        ALL_CALLS_SUPPORTED_SO_FAR = true;
    }
}
