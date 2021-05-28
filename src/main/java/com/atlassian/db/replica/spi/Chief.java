package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.internal.RouteDecisionBuilder;

import java.sql.Connection;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * Can override the automatic decision to make a final call where the query should go.
 */
public interface Chief {
    void overrideDecision(RouteDecisionBuilder decisionBuilder, Collection<Supplier<Connection>> replicaConnections);
}
