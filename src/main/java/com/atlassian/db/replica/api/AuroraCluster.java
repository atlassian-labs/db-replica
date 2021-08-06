package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.aurora.AuroraReplicasDiscoverer;
import com.atlassian.db.replica.spi.DatabaseCluster;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;

public final class AuroraCluster implements DatabaseCluster {
    private final AuroraReplicasDiscoverer discoverer;

    public AuroraCluster(SqlCall<Connection> connection) throws SQLException {
        discoverer = new AuroraReplicasDiscoverer((connection.call()));
    }

    @Override
    public Collection<Database> getReplicas() throws SQLException {
        return discoverer.fetchReplicasServerIds().stream().map(it -> new Database() {
            @Override
            public String getUuid() {
                return it;
            }

            @Override
            public Connection getConnection() {
                return null;
            }
        }).collect(Collectors.toList());
    }

}
