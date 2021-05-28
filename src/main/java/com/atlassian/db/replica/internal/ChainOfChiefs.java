package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.Chief;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class ChainOfChiefs implements Chief {
    private final List<Chief> chiefs;

    public ChainOfChiefs(List<Chief> chiefs) {
        this.chiefs = chiefs;
    }

    @Override
    public void overrideDecision(
        RouteDecisionBuilder decisionBuilder,
        Collection<Supplier<Connection>> replicaConnections
    ) {
        for (Chief chief : chiefs) {
            chief.overrideDecision(decisionBuilder, replicaConnections);
            if (decisionBuilder.build().willRunOnMain()) {
                break;
            }
        }
    }
}
