package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.AuroraConnectionDetails;
import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.api.jdbc.JdbcUrl;
import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.internal.logs.LazyLogger;
import com.atlassian.db.replica.internal.logs.NoopLazyLogger;
import com.atlassian.db.replica.spi.Logger;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class AuroraClusterDiscovery {
    private final ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider;
    private final SuppliedCache<Collection<Database>> discoveredReplicasCache;
    private final String clusterUri;
    private final Logger logger;
    private final LazyLogger lazyLogger;

    private AuroraClusterDiscovery(
        ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider,
        SuppliedCache<Collection<Database>> discoveredReplicasCache,
        String clusterUri,
        Logger logger,
        LazyLogger lazyLogger
    ) {
        this.replicaConnectionPerUrlProvider = replicaConnectionPerUrlProvider;
        this.discoveredReplicasCache = discoveredReplicasCache;
        this.clusterUri = clusterUri;
        this.logger = logger;
        this.lazyLogger = lazyLogger;
    }

    public Collection<Database> getReplicas(Supplier<Connection> connectionSupplier) {
        return discoveredReplicasCache.get(() -> fetchReplicas(connectionSupplier)).orElseGet(() -> {
            lazyLogger.debug(() -> "AuroraClusterDiscovery#getReplicas no replicas cached.");
            return Collections.emptyList();
        });
    }

    private Collection<Database> fetchReplicas(Supplier<Connection> connectionSupplier) {
        try {
            final Connection connection = connectionSupplier.get();
            final AuroraReplicasDiscoverer discoverer = createDiscoverer(connection);
            return discoverer.fetchReplicasUrls(connection).stream().map(auroraUrl -> {
                JdbcUrl url = auroraUrl.toJdbcUrl();
                return new AuroraReplicaNode(auroraUrl.getEndpoint().getServerId(),
                    replicaConnectionPerUrlProvider.getReplicaConnectionProvider(url)
                );
            }).collect(toList());
        } catch (SQLException exception) {
            throw new ReadReplicaDiscoveryOperationException(exception);
        }
    }

    private AuroraReplicasDiscoverer createDiscoverer(Connection connection) {
        if (this.clusterUri != null) {
            return createDiscovererFromClusterUri();
        } else {
            return createDiscovererFromConnection(connection);
        }
    }

    private AuroraReplicasDiscoverer createDiscovererFromConnection(Connection connection) {
        try {
            final String databaseUrl = connection.getMetaData().getURL();
            lazyLogger.debug(() -> format("AuroraClusterDiscovery#createDiscovererFromConnection (databaseUrl=%s)",
                databaseUrl
            ));
            final String[] split = databaseUrl.split("/");
            final String readerEndpoint = split[2];
            final String databaseName = split[3];
            return new AuroraReplicasDiscoverer(new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName),
                logger,
                lazyLogger
            );
        } catch (SQLException exception) {
            throw new ReadReplicaDiscovererCreationException(exception);
        }
    }

    private AuroraReplicasDiscoverer createDiscovererFromClusterUri() {
        lazyLogger.debug(() -> format("AuroraClusterDiscovery#createDiscovererFromClusterUri (clusterUri=%s)",
            clusterUri
        ));
        final String[] split = clusterUri.split("/");
        final String readerEndpoint = split[2];
        final String databaseName = split[3];
        return new AuroraReplicasDiscoverer(new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName),
            logger,
            lazyLogger
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider;
        private SuppliedCache<Collection<Database>> discoveredReplicasCache = new NoCacheSuppliedCache<>();
        private String clusterUri;
        private Logger logger;
        private LazyLogger lazyLogger = new NoopLazyLogger();

        public Builder replicaConnectionPerUrlProvider(ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider) {
            this.replicaConnectionPerUrlProvider = replicaConnectionPerUrlProvider;
            return this;
        }

        public Builder discoveredReplicasCache(SuppliedCache<Collection<Database>> discoveredReplicasCache) {
            this.discoveredReplicasCache = discoveredReplicasCache;
            return this;
        }

        public Builder clusterUri(String clusterUri) {
            this.clusterUri = clusterUri;
            return this;
        }

        /**
         * @param logger
         * @return
         * @deprecated use {@link Builder#logger(LazyLogger)}
         */
        @Deprecated
        public Builder logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder logger(LazyLogger lazyLogger) {
            this.lazyLogger = lazyLogger;
            return this;
        }

        /**
         * @deprecated use
         * {@link Builder#replicaConnectionPerUrlProvider(ReplicaConnectionPerUrlProvider)}{@code .}
         * {@link Builder#build()} instead.
         * also see {@link AuroraConnectionDetails}.
         */
        @Deprecated
        public AuroraClusterDiscovery build(AuroraConnectionDetails auroraConnectionDetails) {
            return replicaConnectionPerUrlProvider(auroraConnectionDetails.convert()).build();
        }

        public AuroraClusterDiscovery build() {
            return new AuroraClusterDiscovery(replicaConnectionPerUrlProvider,
                discoveredReplicasCache,
                clusterUri,
                logger,
                lazyLogger
            );
        }
    }

}
