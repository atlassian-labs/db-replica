package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.circuitbreaker.CircuitBreaker;
import com.atlassian.db.replica.api.circuitbreaker.DualConnectionException;

import java.sql.SQLException;

public class BreakerHandler {
    private final CircuitBreaker breaker;

    public BreakerHandler(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    public <T> T handle(SqlCall<T> call) {
        try {
            return call.call();
        } catch (Throwable throwable) {
            breaker.handle(throwable);
            throw new DualConnectionException("Db replica call failed. CircuitBreaker is " + breaker.getState(), throwable);
        }
    }

    public void handle(SqlRunnable call) {
        try {
            call.run();
        } catch (Throwable throwable) {
            breaker.handle(throwable);
            throw new DualConnectionException("Db replica call failed. CircuitBreaker is " + breaker.getState(), throwable);
        }
    }

    public interface SqlRunnable {
        void run() throws SQLException;
    }
}
