package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.AuroraConnectionDetails;
import com.atlassian.db.replica.internal.Database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public final class AuroraClusterDiscovery {
    private final AuroraConnectionDetails auroraConnectionDetails;

    public AuroraClusterDiscovery(AuroraConnectionDetails auroraConnectionDetails) {
        this.auroraConnectionDetails = auroraConnectionDetails;
    }

    public Collection<Database> getReplicas(Supplier<Connection> connectionSupplier) {
        try (final Connection connection = connectionSupplier.get()) {
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

}
