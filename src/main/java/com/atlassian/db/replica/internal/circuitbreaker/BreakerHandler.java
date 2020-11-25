package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;

import java.sql.SQLException;

public class BreakerHandler {
    private final CircuitBreaker breaker;

    public BreakerHandler(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    public <T> T handle(SqlCall<T> call) throws SQLException {
        try {
            return call.call();
        } catch (Throwable throwable) {
            breaker.handle(throwable);
            throw throwable;
        }
    }

    public void handle(SqlRunnable call) throws SQLException {
        try {
            call.run();
        } catch (Throwable throwable) {
            breaker.handle(throwable);
            throw throwable;
        }
    }

    public interface SqlRunnable {
        void run() throws SQLException;
    }
}
