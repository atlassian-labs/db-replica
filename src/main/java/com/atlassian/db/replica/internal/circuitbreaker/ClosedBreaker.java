package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.CircuitBreaker;
import com.atlassian.db.replica.api.SqlRun;

import java.sql.SQLException;

public class ClosedBreaker implements CircuitBreaker {

    @Override
    public boolean canCreateDualConnection() {
        return true;
    }

    @Override
    public <T> T handle(SqlCall<T> call) throws SQLException {
        return call.call();
    }

    @Override
    public void handle(SqlRun run) throws SQLException {
        run.run();
    }
}
