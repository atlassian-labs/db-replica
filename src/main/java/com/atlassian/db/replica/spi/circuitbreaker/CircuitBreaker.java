package com.atlassian.db.replica.spi.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;

import java.sql.SQLException;

public interface CircuitBreaker {

    boolean canCall();

    <T> T handle(SqlCall<T> call) throws SQLException;

    void handle(SqlRunnable runnable) throws SQLException;

    interface SqlRunnable {
        void run() throws SQLException;
    }
}
