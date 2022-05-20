package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.api.jdbc.JdbcUrl;
import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.spi.DataSource;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import static java.util.stream.Collectors.toList;

public final class AuroraClusterDiscovery {
    private final ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider;
    private final SuppliedCache<Collection<Database>> discoveredReplicasCache;
    private final String clusterUri;

    private AuroraClusterDiscovery(
        ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider,
        SuppliedCache<Collection<Database>> discoveredReplicasCache,
        String clusterUri
    ) {
        this.replicaConnectionPerUrlProvider = replicaConnectionPerUrlProvider;
        this.discoveredReplicasCache = discoveredReplicasCache;
        this.clusterUri = clusterUri;
    }

    public Collection<Database> getReplicas(DataSource dataSource) {
        return discoveredReplicasCache.get(() -> fetchReplicas(dataSource))
            .orElse(Collections.emptyList());
    }

    private Collection<Database> fetchReplicas(DataSource dataSource) {
        try {
            final Connection connection = dataSource.getConnection();
            final AuroraReplicasDiscoverer discoverer = createDiscoverer(connection);
            return discoverer.fetchReplicasUrls(connection).stream()
                .map(auroraUrl -> {
                    JdbcUrl url = auroraUrl.toJdbcUrl();
                    return new AuroraReplicaNode(
                        auroraUrl.getEndpoint().getServerId(),
                        replicaConnectionPerUrlProvider.getReplicaConnectionProvider(url)
                    );
                })
                .collect(toList());
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
            final String[] split = databaseUrl.split("/");
            final String readerEndpoint = split[2];
            final String databaseName = split[3];
            return new AuroraReplicasDiscoverer(
                new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName)
            );
        } catch (SQLException exception) {
            throw new ReadReplicaDiscovererCreationException(exception);
        }
    }

    private AuroraReplicasDiscoverer createDiscovererFromClusterUri() {
        final String[] split = clusterUri.split("/");
        final String readerEndpoint = split[2];
        final String databaseName = split[3];
        return new AuroraReplicasDiscoverer(
            new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName)
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider;
        private SuppliedCache<Collection<Database>> discoveredReplicasCache = new NoCacheSuppliedCache<>();
        private String clusterUri;

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

        public AuroraClusterDiscovery build() {
            return new AuroraClusterDiscovery(replicaConnectionPerUrlProvider, discoveredReplicasCache, clusterUri);
        }
    }

}
