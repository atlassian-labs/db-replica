package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.AuroraConnectionDetails;
import com.atlassian.db.replica.internal.Database;
import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public final class AuroraClusterDiscovery {
    private final AuroraConnectionDetails auroraConnectionDetails;
    private final SuppliedCache<Collection<Database>> discoveredReplicasCache;

    private AuroraClusterDiscovery(
        AuroraConnectionDetails auroraConnectionDetails,
        SuppliedCache<Collection<Database>> discoveredReplicasCache
    ) {
        this.auroraConnectionDetails = auroraConnectionDetails;
        this.discoveredReplicasCache = discoveredReplicasCache;
    }

    public Collection<Database> getReplicas(Supplier<Connection> connectionSupplier) {
        return discoveredReplicasCache.get(() -> fetchReplicas(connectionSupplier))
            .orElse(Collections.emptyList());
    }

    private Collection<Database> fetchReplicas(Supplier<Connection> connectionSupplier) {
        try {
            final Connection connection = connectionSupplier.get();
            final AuroraReplicasDiscoverer discoverer = createDiscoverer(connection);
            return discoverer.fetchReplicasUrls(connection).stream()
                .map(auroraUrl -> new AuroraReplicaNode(auroraUrl, auroraConnectionDetails))
                .collect(toList());
        } catch (SQLException exception) {
            throw new ReadReplicaDiscoveryOperationException(exception);
        }
    }

    private AuroraReplicasDiscoverer createDiscoverer(Connection connection) {
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private SuppliedCache<Collection<Database>> discoveredReplicasCache = new NoCacheSuppliedCache<>();

        public Builder discoveredReplicasCache(SuppliedCache<Collection<Database>> discoveredReplicasCache) {
            this.discoveredReplicasCache = discoveredReplicasCache;
            return this;
        }

        public AuroraClusterDiscovery build(AuroraConnectionDetails auroraConnectionDetails) {
            return new AuroraClusterDiscovery(auroraConnectionDetails, discoveredReplicasCache);
        }
    }

}
