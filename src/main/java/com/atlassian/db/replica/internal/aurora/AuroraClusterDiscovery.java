package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public final class AuroraClusterDiscovery {
    private final ReplicaNode replicaNode;
    private final AuroraReplicasDiscoverer discoverer;

    public AuroraClusterDiscovery(
        String readerEndpoint,
        String databaseName
    ) {
        this.replicaNode = new ReplicaNode();
        this.discoverer = new AuroraReplicasDiscoverer(
            new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName)
        );
    }

    public Collection<Database> getReplicas(Supplier<Connection> connectionSupplier) {
        try {
            final Connection connection = connectionSupplier.get();
            return discoverer.fetchReplicasUrl(connection).stream().map(auroraUrl -> new Database() {
                @Override
                public String getId() {
                    return auroraUrl.getEndpoint().getServerId();
                }

                @Override
                public Supplier<Connection> getConnectionSupplier() {
                    return () -> {
                        try {
                            return replicaNode.mark(
                                getConnection(auroraUrl),
                                auroraUrl.getEndpoint().getServerId()
                            );
                        } catch (SQLException throwables) {
                            throw new RuntimeException("TODO");
                        }
                    };
                }

                private Connection getConnection(AuroraJdbcUrl url) throws SQLException {
                    final Properties props = new Properties();
                    props.setProperty("user", "postgres");
                    props.setProperty("password", System.getenv("password"));
                    return DriverManager.getConnection(url.toString(), props);
                }
            }).collect(toList());
        } catch (SQLException throwables) {
            throw new RuntimeException("TODO");
        }

    }

}
