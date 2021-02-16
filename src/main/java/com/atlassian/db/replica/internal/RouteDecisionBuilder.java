package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.api.reason.RouteDecision;

import java.util.Objects;

public final class RouteDecisionBuilder {
    private String sql = null;
    private Reason reason;
    private RouteDecision cause = null;

    public RouteDecisionBuilder(Reason reason) {
        this.reason = reason;
    }

    public RouteDecisionBuilder sql(final String sql) {
        this.sql = sql;
        return this;
    }

    public RouteDecisionBuilder reason(final Reason reason) {
        this.reason = reason;
        return this;
    }

    public RouteDecisionBuilder cause(final RouteDecision cause) {
        this.cause = cause;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public RouteDecision build() {
        return new RouteDecision(sql, reason, cause);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RouteDecisionBuilder that = (RouteDecisionBuilder) o;
        return Objects.equals(sql, that.sql)
            && Objects.equals(reason, that.reason)
            && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, reason, cause);
    }

    @Override
    public String toString() {
        return "RouteDecisionBuilder{" +
            "sql='" + sql + '\'' +
            ", reason=" + reason +
            ", cause=" + cause +
            '}';
    }
}
