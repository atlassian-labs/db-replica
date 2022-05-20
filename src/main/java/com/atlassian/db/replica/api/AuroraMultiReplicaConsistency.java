package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.internal.NotLoggingLogger;
import com.atlassian.db.replica.internal.aurora.AuroraClusterDiscovery;
import com.atlassian.db.replica.internal.aurora.ReadReplicaConnectionCreationException;
import com.atlassian.db.replica.spi.Logger;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.util.Collection;

public final class AuroraMultiReplicaConsistency implements ReplicaConsistency {
    private final Logger logger;
    private final ReplicaConsistency replicaConsistency;
    private final AuroraClusterDiscovery cluster;

    private AuroraMultiReplicaConsistency(
        Logger logger,
        ReplicaConsistency replicaConsistency,
        ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider,
        SuppliedCache<Collection<Database>> discoveredReplicasCache,
        String clusterUri
    ) {
        this.logger = logger;
        this.replicaConsistency = replicaConsistency;
        this.cluster = AuroraClusterDiscovery.builder()
            .replicaConnectionPerUrlProvider(replicaConnectionPerUrlProvider)
            .discoveredReplicasCache(discoveredReplicasCache)
            .clusterUri(clusterUri)
            .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void write(Connection main) {
        replicaConsistency.write(main);
    }

    @Override
    public boolean isConsistent(Database database) {
        return cluster.getReplicas(database.getDataSource())
            .stream()
            .allMatch(replica -> {
                try {
                    return replicaConsistency.isConsistent(replica);
                } catch (ReadReplicaConnectionCreationException exception) {
                    logger.warn(
                        "ReadReplicaConnectionCreationException occurred during consistency checking. It is likely that replica is the process of scaling, replica id: " + replica.getId(),
                        exception
                    );
                    return true;
                }
            });
    }

    public static final class Builder {
        private ReplicaConsistency replicaConsistency;
        private ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider;
        private SuppliedCache<Collection<Database>> discoveredReplicasCache = new NoCacheSuppliedCache<>();
        private Logger logger = new NotLoggingLogger();
        private String clusterUri;

        public Builder replicaConsistency(ReplicaConsistency replicaConsistency) {
            this.replicaConsistency = replicaConsistency;
            return this;
        }

        public Builder replicaConnectionPerUrlProvider(ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider) {
            this.replicaConnectionPerUrlProvider = replicaConnectionPerUrlProvider;
            return this;
        }

        public Builder logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder clusterUri(String clusterUri) {
            this.clusterUri = clusterUri;
            return this;
        }

        /**
         * Puts this connection in compatibility mode with a previous version. Developers can use this method to
         * roll out the new version of the library with a feature flag.
         * <p>
         * It's best-effort, and there's no guarantee the library in compatibility mode will always behave
         * the same way as the previous version of the library.
         */
        public Builder compatibleWithPreviousVersion() {
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

        public AuroraMultiReplicaConsistency build() {
            return new AuroraMultiReplicaConsistency(
                logger,
                replicaConsistency,
                replicaConnectionPerUrlProvider,
                discoveredReplicasCache,
                clusterUri
            );
        }
    }
}
