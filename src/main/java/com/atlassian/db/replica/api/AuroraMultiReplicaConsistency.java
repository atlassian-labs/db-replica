package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.exception.ConnectionCouldNotBeClosedException;
import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.internal.NotLoggingLogger;
import com.atlassian.db.replica.internal.aurora.AuroraClusterDiscovery;
import com.atlassian.db.replica.internal.aurora.ReadReplicaConnectionCreationException;
import com.atlassian.db.replica.spi.Logger;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.function.Supplier;

public final class AuroraMultiReplicaConsistency implements ReplicaConsistency {
    private final Logger logger;
    private final ReplicaConsistency replicaConsistency;
    private final AuroraClusterDiscovery cluster;

    private AuroraMultiReplicaConsistency(
        Logger logger,
        ReplicaConsistency replicaConsistency,
        ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider,
        SuppliedCache<Collection<Database>> discoveredReplicasCache
    ) {
        this.logger = logger;
        this.replicaConsistency = replicaConsistency;
        this.cluster = AuroraClusterDiscovery.builder()
            .replicaConnectionPerUrlProvider(replicaConnectionPerUrlProvider)
            .discoveredReplicasCache(discoveredReplicasCache)
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
    public boolean isConsistent(Supplier<Connection> replicaSupplier) {
        return cluster.getReplicas(replicaSupplier)
            .stream()
            .allMatch(replica -> {
                try (Connection connection = replica.getConnectionSupplier().get()) {
                    return replicaConsistency.isConsistent(() -> connection);
                } catch (ReadReplicaConnectionCreationException exception) {
                    logger.warn("ReadReplicaConnectionCreationException occurred during consistency checking. It is likely that replica is the process of scaling, replica id: " + replica.getId(), exception);
                    return true;
                } catch (SQLException exception) {
                    throw new ConnectionCouldNotBeClosedException(exception);
                }
            });
    }

    public static final class Builder {
        private ReplicaConsistency replicaConsistency;
        private ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider;
        private SuppliedCache<Collection<Database>> discoveredReplicasCache = new NoCacheSuppliedCache<>();
        private Logger logger = new NotLoggingLogger();

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

        /**
         * It takes #{{@link SuppliedCache}} implementation to store fetched replicas in the cache. This cache should
         * consider balance between overhead (with no cache we are fetching replicas with every
         * #{AuroraMultiReplicaConsistency.isConsistent} call) and latency in discovering replica cluster changes.
         */
        public Builder discoveredReplicasCache(SuppliedCache<Collection<Database>> discoveredReplicasCache) {
            this.discoveredReplicasCache = discoveredReplicasCache;
            return this;
        }

        /**
         * @deprecated see {@link AuroraConnectionDetails}.
         */
        @Deprecated
        public Builder auroraConnectionDetails(AuroraConnectionDetails auroraConnectionDetails) {
            return replicaConnectionPerUrlProvider(auroraConnectionDetails.convert());
        }

        /**
         * @deprecated use {@link AuroraMultiReplicaConsistency#builder()} instead.
         */
        @Deprecated
        public static Builder anAuroraMultiReplicaConsistencyBuilder() {
            return new Builder();
        }

        public AuroraMultiReplicaConsistency build() {
            return new AuroraMultiReplicaConsistency(
                logger,
                replicaConsistency,
                replicaConnectionPerUrlProvider,
                discoveredReplicasCache
            );
        }
    }
}
