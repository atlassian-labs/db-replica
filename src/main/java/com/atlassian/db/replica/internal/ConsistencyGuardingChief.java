package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.Chief;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.util.Collection;
import java.util.function.Supplier;

import static com.atlassian.db.replica.api.reason.Reason.REPLICA_INCONSISTENT;

public class ConsistencyGuardingChief implements Chief {
    private final ReplicaConsistency replicaConsistency;

    public ConsistencyGuardingChief(ReplicaConsistency replicaConsistency) {
        this.replicaConsistency = replicaConsistency;
    }

    @Override
    public void overrideDecision(
        RouteDecisionBuilder decisionBuilder,
        Collection<Supplier<Connection>> replicaConnections
    ) {
        final Supplier<Connection> firstReplica = replicaConnections.iterator().next();
        if (!replicaConsistency.isConsistent(firstReplica)) {
            decisionBuilder.reason(REPLICA_INCONSISTENT);
        }
    }
}
