package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.AuroraConnectionDetails;
import com.atlassian.db.replica.internal.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

public class AuroraReplicaNode implements Database {

    private final AuroraJdbcUrl auroraUrl;
    private final AuroraConnectionDetails auroraConnectionDetails;
    private final ReplicaNode replicaNode = new ReplicaNode();

    public AuroraReplicaNode(
        AuroraJdbcUrl auroraUrl,
        AuroraConnectionDetails auroraConnectionDetails
    ) {
        this.auroraUrl = auroraUrl;
        this.auroraConnectionDetails = auroraConnectionDetails;
    }

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
            } catch (SQLException exception) {
                throw new ReadReplicaConnectionCreationException(exception);
            }
        };
    }

    private Connection getConnection(AuroraJdbcUrl url) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", auroraConnectionDetails.getUsername());
        properties.setProperty("password", auroraConnectionDetails.getPassword());
        return DriverManager.getConnection(url.toString(), properties);
    }
}
