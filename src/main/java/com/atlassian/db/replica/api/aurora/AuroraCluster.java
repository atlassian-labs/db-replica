package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.internal.aurora.AuroraEndpoint;
import com.atlassian.db.replica.internal.aurora.AuroraJdbcUrl;
import com.atlassian.db.replica.internal.aurora.AuroraReplicasDiscoverer;
import com.atlassian.db.replica.internal.aurora.ReplicaNode;
import com.atlassian.db.replica.spi.DatabaseCluster;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public final class AuroraCluster implements DatabaseCluster {
    private final AuroraReplicasDiscoverer discoverer;
    private final ReplicaNode replicaNode;

    public AuroraCluster(
        SqlCall<Connection> connection,
        String readerEndpoint,
        String databaseName
    ) throws SQLException {
        discoverer = new AuroraReplicasDiscoverer(
            connection.call(),
            new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName)
        );
        this.replicaNode = new ReplicaNode();
    }

    @Override
    public Collection<Database> getReplicas() throws SQLException {
        return discoverer.fetchReplicasUrl().stream().map(auroraUrl -> new Database() {
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
    }

}
