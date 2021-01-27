package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.SqlRun;
import com.atlassian.db.replica.internal.util.ThreadSafe;

import java.sql.SQLException;

/**
 * Decides if it's safe to create another {@link DualConnection}.
 * Can choose to try-catch, time, retry, block or do anything with calls and runs they handle.
 * Can change their state to aid their decision. The calls and retries can come from different threads.
 */
@ThreadSafe
public interface CircuitBreaker {

    /**
     * @return {@code true} if a new {@link DualConnection} is allowed
     */
    boolean canCreateDualConnection();

    /**
     * Wraps the {@code call}.
     * @param call a call within {@link DualConnection} monitored by the circuit breaker
     * @param <T> return type of the {@code call}
     * @return the return value of the {@code call}
     * @throws SQLException for convenience if the implementation rethrows the exception from the {@code call}
     */
    <T> T handle(SqlCall<T> call) throws SQLException;

    /**
     * Wraps the {@code run}.
     * @param run a run within {@link DualConnection} monitored by the circuit breaker
     * @throws SQLException for convenience if the implementation rethrows the exception from the {@code run}
     */
    void handle(SqlRun run) throws SQLException;
}
