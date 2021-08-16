package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.internal.aurora.AuroraReplicasDiscoverer;
import com.atlassian.db.replica.internal.aurora.ReplicaNode;
import com.atlassian.db.replica.spi.DatabaseCluster;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class AuroraCluster implements DatabaseCluster {
    private final AuroraReplicasDiscoverer discoverer;
    private final ReplicaNode replicaNode;

    public AuroraCluster(SqlCall<Connection> connection) throws SQLException {
        discoverer = new AuroraReplicasDiscoverer((connection.call()));
        this.replicaNode = new ReplicaNode();
    }

    @Override
    public Collection<Database> getReplicas() throws SQLException {
        return discoverer.fetchReplicasServerIds().stream().map(replicaId -> new Database() {
            @Override
            public String getId() {
                return replicaId;
            }

            @Override
            public Supplier<Connection> getConnectionSupplier() {
                return () -> {
                    try {
                        return replicaNode.mark(getConnection(replicaId), replicaId);
                    } catch (SQLException throwables) {
                        throw new RuntimeException("TODO");
                    }
                };
            }

            private Connection getConnection(String databaseId) throws SQLException {
                final String url = "jdbc:postgresql://" + databaseId + ".crmnlihjxqlm.eu-central-1.rds.amazonaws.com:5432/newdb"; //TODO remove hardcoded values
                final Properties props = new Properties();
                props.setProperty("user", "postgres");
                props.setProperty("password", System.getenv("password"));
                return DriverManager.getConnection(url, props);
            }
        }).collect(Collectors.toList());
    }

}
