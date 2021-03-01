package com.atlassian.db.replica.api.reason;

import java.util.Objects;
import java.util.Optional;

import static com.atlassian.db.replica.api.reason.Reason.LOCK;
import static com.atlassian.db.replica.api.reason.Reason.RW_API_CALL;
import static com.atlassian.db.replica.api.reason.Reason.WRITE_OPERATION;

/**
 * Reveals details related to why, and which database will be used.
 */
public final class RouteDecision {
    private final Reason reason;
    private final String sql;
    private final RouteDecision cause;

    public RouteDecision(String sql, Reason reason, RouteDecision cause) {
        this.sql = sql;
        this.reason = reason;
        this.cause = cause;
    }

    /**
     * @return Reason for the current route. The state of the connection may enforce it.
     */
    public Reason getReason() {
        return reason;
    }

    /**
     * @return An SQL corresponding to the current route, if any.
     */
    public Optional<String> getSql() {
        return Optional.ofNullable(sql);
    }

    /**
     * @return The initial decision to change the state, if any.
     */
    public Optional<RouteDecision> getCause() {
        return Optional.ofNullable(cause);
    }

    /**
     * @return information whether reason was caused by write sql operation.
     */
    public boolean isWrite(){
        return WRITE_OPERATION.equals(reason) ||
                LOCK.equals(reason) ||
                RW_API_CALL.equals(reason);
    }

    /**
     * @return whether sql was run on Main
     */
    public boolean isRunOnMain() {
        return reason.isRunOnMain();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RouteDecision that = (RouteDecision) o;
        return Objects.equals(reason, that.reason) && Objects.equals(
                sql,
                that.sql
        ) && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason, sql, cause);
    }

    @Override
    public String toString() {
        return "RouteDecision{" +
                "reason=" + reason +
                ", sql='" + sql + '\'' +
                ", cause=" + cause +
                '}';
    }
}
