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
    private final ReplicaNode replicaNode = new ReplicaNode();

    public Collection<Database> getReplicas(Supplier<Connection> connectionSupplier) {
        try {
            final Connection connection = connectionSupplier.get();
            final AuroraReplicasDiscoverer discoverer = createDiscoverer(connection);
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

    private AuroraReplicasDiscoverer createDiscoverer(Connection connection) {
        try {
            final String dbUrl = connection.getMetaData().getURL();
            final String[] split = dbUrl.split("/");
            final String readerEndpoint = split[2];
            final String databaseName = split[3];
            return new AuroraReplicasDiscoverer(
                new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName)
            );
        } catch (SQLException throwables) {
            throw new RuntimeException("TODO", throwables);
        }
    }

}
