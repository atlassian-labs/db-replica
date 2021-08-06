package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.internal.aurora.AuroraReplicasDiscoverer;
import com.atlassian.db.replica.spi.DatabaseCluster;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;

public final class AuroraCluster implements DatabaseCluster {
    private final AuroraReplicasDiscoverer discoverer;
    private final ReplicaNode replicaNode;

    public AuroraCluster(SqlCall<Connection> connection, ReplicaNode replicaNode) throws SQLException {
        discoverer = new AuroraReplicasDiscoverer((connection.call()));
        this.replicaNode = replicaNode;
    }

    @Override
    public Collection<Database> getReplicas() throws SQLException {
        return discoverer.fetchReplicasServerIds().stream().map(it -> new Database() {
            @Override
            public String getUuid() {
                return it;
            }

            @Override
            public SqlCall<Connection> getConnection() {
                return () -> replicaNode.mark(getConnection(it));//TODO log failures
            }

            private Connection getConnection(String databaseId) throws SQLException {
                final String url = "jdbc:postgresql://" + databaseId + ".crmnlihjxqlm.eu-central-1.rds.amazonaws.com:5432/newdb"; //TODO remove hardcoded values
                final Properties props = new Properties();
                props.setProperty("user", "postgres");
                props.setProperty("password", System.getenv("password"));
                Connection conn = DriverManager.getConnection(url, props);
                return conn;
            }
        }).collect(Collectors.toList());
    }

}
