package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.internal.ReadReplicaUnsupportedOperationException;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class BreakOnNotSupportedOperations implements CircuitBreaker {

    private static volatile boolean LACK_OF_SUPPORT_DETECTED = false;

    @Override
    public boolean canCall() {
        return !LACK_OF_SUPPORT_DETECTED;
    }

    @Override
    public <T> T handle(SqlCall<T> call) throws SQLException {
        try {
            return call.call();
        } catch (ReadReplicaUnsupportedOperationException | SQLFeatureNotSupportedException e) {
            LACK_OF_SUPPORT_DETECTED = true;
            throw e;
        }
    }

    @Override
    public void handle(SqlRunnable runnable) throws SQLException {
        try {
            runnable.run();
        } catch (ReadReplicaUnsupportedOperationException | SQLFeatureNotSupportedException e) {
            LACK_OF_SUPPORT_DETECTED = true;
            throw e;
        }
    }

    public static void reset() {
        LACK_OF_SUPPORT_DETECTED = false;
    }
}
