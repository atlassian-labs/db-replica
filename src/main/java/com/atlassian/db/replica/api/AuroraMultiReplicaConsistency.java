package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.aurora.AuroraClusterDiscovery;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.util.function.Supplier;

public final class AuroraMultiReplicaConsistency implements ReplicaConsistency {
    private final ReplicaConsistency replicaConsistency;
    private final AuroraClusterDiscovery cluster;

    private AuroraMultiReplicaConsistency(
        ReplicaConsistency replicaConsistency,
        AuroraConnectionDetails auroraConnectionDetails
    ) {
        this.replicaConsistency = replicaConsistency;
        this.cluster = new AuroraClusterDiscovery(auroraConnectionDetails);
    }

    @Override
    public void write(Connection main) {
        replicaConsistency.write(main);
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replicaSupplier) {
        return cluster.getReplicas(replicaSupplier).stream()
            .allMatch(replica -> replicaConsistency.isConsistent(() -> replica.getConnectionSupplier().get()));
    }

    public static final class Builder {
        private ReplicaConsistency replicaConsistency;
        private AuroraConnectionDetails auroraConnectionDetails;

        public static Builder anAuroraMultiReplicaConsistencyBuilder() {
            return new Builder();
        }

        public Builder replicaConsistency(ReplicaConsistency replicaConsistency) {
            this.replicaConsistency = replicaConsistency;
            return this;
        }

        public Builder auroraConnectionDetails(AuroraConnectionDetails auroraConnectionDetails) {
            this.auroraConnectionDetails = auroraConnectionDetails;
            return this;
        }

        public ReplicaConsistency build() {
            return new AuroraMultiReplicaConsistency(replicaConsistency, auroraConnectionDetails);
        }
    }

}
