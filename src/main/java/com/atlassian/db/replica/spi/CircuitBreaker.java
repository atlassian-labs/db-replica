package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.SqlRun;

import java.sql.SQLException;

public interface CircuitBreaker {

    boolean canCall();

    <T> T handle(SqlCall<T> call) throws SQLException;

    void handle(SqlRun run) throws SQLException;
}
