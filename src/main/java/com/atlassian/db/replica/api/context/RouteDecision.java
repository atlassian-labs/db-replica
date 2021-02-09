package com.atlassian.db.replica.api.context;


import java.util.Objects;
import java.util.Optional;

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
     * @return An SQL corresponding to the current route. It can be `null`.
     */
    public Optional<String> getSql() {
        return Optional.ofNullable(sql);
    }

    /**
     * @return If the decision was affected by the current connection state,
     * it returns the initial decision to change the state. Otherwise, it returns `null`.
     */
    public RouteDecision getCause() {
        return cause;
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
