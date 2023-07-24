package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.exception.ConnectionCouldNotBeClosedException;
import com.atlassian.db.replica.internal.LazyReference;
import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.internal.NotLoggingLogger;
import com.atlassian.db.replica.internal.aurora.AuroraClusterDiscovery;
import com.atlassian.db.replica.api.exception.ReadReplicaConnectionCreationException;
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
        SuppliedCache<Collection<Database>> discoveredReplicasCache,
        String clusterUri
    ) {
        this.logger = logger;
        this.replicaConsistency = replicaConsistency;
        this.cluster = AuroraClusterDiscovery.builder()
            .replicaConnectionPerUrlProvider(replicaConnectionPerUrlProvider)
            .discoveredReplicasCache(discoveredReplicasCache)
            .clusterUri(clusterUri)
            .logger(logger)
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

        Collection<Database> replicas = cluster.getReplicas(replicaSupplier);
        logger.info("Checking consistency for " + replicas.size() + " replicas.");

        return replicas.stream()
            .allMatch(replica -> {
                logger.info("Checking consistency for replica:" + replica.getId());
                try (LazyConnectionSupplier connectionSupplier = new LazyConnectionSupplier(replica.getConnectionSupplier())) {
                    return replicaConsistency.isConsistent(connectionSupplier);
                } catch (ReadReplicaConnectionCreationException exception) {
                    logger.warn(
                        "ReadReplicaConnectionCreationException occurred during consistency checking. It is likely that replica is the process of scaling, replica id: " + replica.getId(),
                        exception
                    );
                    return true;
                } catch (SQLException exception) {
                    throw new ConnectionCouldNotBeClosedException(exception);
                }
            });
    }

    private static class LazyConnectionSupplier implements Supplier<Connection>, AutoCloseable {
        final LazyReference<Connection> connectionLazyReference = new LazyReference<Connection>() {
            @Override
            protected Connection create() {
                return supplier.get();
            }
        };
        private final Supplier<Connection> supplier;

        private LazyConnectionSupplier(Supplier<Connection> supplier) {
            this.supplier = supplier;
        }

        /**
         * The initial implementation of AuroraMultiReplicaConsistency was buggy. The implementation took the
         * connection management responsibility from ReplicaConsistency. The current implementation fixes the issue
         * by allowing ReplicaConsistency implementation to take full responsibility for the connection lifecycle.
         * <p>
         * Unfortunately, the existing implementations of ReplicaConsistency may rely on the bug (for example some
         * implementations in our product rely on it). To make it backwards compatible (behavioural compatibility),
         * LazyConnectionSupplier closes the connection if ReplicaConsistency doesn't do that.
         */
        @Override
        public void close() throws SQLException {
            if (connectionLazyReference.isInitialized()) {
                final Connection connection = connectionLazyReference.get();
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
        }

        @Override
        public Connection get() {
            return connectionLazyReference.get();
        }
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
                discoveredReplicasCache,
                clusterUri
            );
        }
    }
}
