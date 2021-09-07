package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.Database;
import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.internal.aurora.AuroraClusterDiscovery;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.util.Collection;
import java.util.function.Supplier;

public final class AuroraMultiReplicaConsistency implements ReplicaConsistency {
    private final ReplicaConsistency replicaConsistency;
    private final AuroraClusterDiscovery cluster;

    private AuroraMultiReplicaConsistency(
        ReplicaConsistency replicaConsistency,
        AuroraConnectionDetails auroraConnectionDetails,
        SuppliedCache<Collection<Database>> discoveredReplicasCache
    ) {
        this.replicaConsistency = replicaConsistency;
        this.cluster = AuroraClusterDiscovery.builder()
            .discoveredReplicasCache(discoveredReplicasCache)
            .build(auroraConnectionDetails);
    }

    @Override
    public void write(Connection main) {
        replicaConsistency.write(main);
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replicaSupplier) {
        Collection<Database> replicas = cluster.getReplicas(replicaSupplier);

        if (replicas.isEmpty()) {
            return false;
        }

        return replicas.stream()
            .allMatch(replica -> replicaConsistency.isConsistent(() -> replica.getConnectionSupplier().get()));
    }

    public static final class Builder {
        private ReplicaConsistency replicaConsistency;
        private AuroraConnectionDetails auroraConnectionDetails;
        private SuppliedCache<Collection<Database>> discoveredReplicasCache = new NoCacheSuppliedCache<>();

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

        /**
         * It takes #{{@link SuppliedCache}} implementation to store fetched replicas in the cache. This cache should
         * consider balance between overhead (with no cache we are fetching replicas with every
         * #{AuroraMultiReplicaConsistency.isConsistent} call) and latency in discovering replica cluster changes.
         */
        public Builder discoveredReplicasCache(SuppliedCache<Collection<Database>> discoveredReplicasCache) {
            this.discoveredReplicasCache = discoveredReplicasCache;
            return this;
        }

        public ReplicaConsistency build() {
            return new AuroraMultiReplicaConsistency(
                replicaConsistency,
                auroraConnectionDetails,
                discoveredReplicasCache
            );
        }
    }

}
