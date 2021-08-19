package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.internal.aurora.AuroraClusterDiscovery;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.util.function.Supplier;

//TODO: promote to API?
public class AuroraMultiReplicaConsistency implements ReplicaConsistency {
    private final ReplicaConsistency replicaConsistency;
    private final AuroraClusterDiscovery cluster;

    public AuroraMultiReplicaConsistency(
        ReplicaConsistency replicaConsistency
    ) {
        this.replicaConsistency = replicaConsistency;
        this.cluster = new AuroraClusterDiscovery();
    }

    @Override
    public void write(Connection main) {
        replicaConsistency.write(main);
    }

    // TODO: currently it ignores the provided supplier and uses cluster. We could change the API to provide all
    //    // connections
    @Override
    public boolean isConsistent(Supplier<Connection> replicaSupplier) {
        return cluster.getReplicas(replicaSupplier).stream()
            .allMatch(replica -> replicaConsistency.isConsistent(() -> replica.getConnectionSupplier().get()));
    }
}
