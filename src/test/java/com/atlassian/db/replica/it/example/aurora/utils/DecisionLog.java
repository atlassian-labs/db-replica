package com.atlassian.db.replica.it.example.aurora.utils;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.spi.DatabaseCall;
import com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.util.List;

public final class DecisionLog implements DatabaseCall {
    private final ImmutableList.Builder<RouteDecision> decisions = new ImmutableList.Builder<>();

    @Override
    public <T> T call(SqlCall<T> call, RouteDecision decision) throws SQLException {
        decisions.add(decision);
        return call.call();
    }

    public List<RouteDecision> getDecisions() {
        return decisions.build();
    }
}
