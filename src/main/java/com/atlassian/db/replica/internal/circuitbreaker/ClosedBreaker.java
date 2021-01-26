package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;

import java.sql.SQLException;

public class ClosedBreaker implements CircuitBreaker {

    @Override
    public boolean canCall() {
        return true;
    }

    @Override
    public <T> T handle(SqlCall<T> call) throws SQLException {
        return call.call();
    }

    @Override
    public void handle(SqlRunnable runnable) throws SQLException {
        runnable.run();
    }
}
