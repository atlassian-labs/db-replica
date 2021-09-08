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
                    getConnection(auroraUrl.toString()),
                    auroraUrl.getEndpoint().getServerId()
                );
            } catch (SQLException exception) {
                throw new ReadReplicaConnectionCreationException(exception);
            }
        };
    }

    private Connection getConnection(String url) throws SQLException {
        final String[] urlSplit = url.replace("postgresql://", "").split("@");
        final String userPassword = urlSplit[0];
        final String[] userPasswordSplit = userPassword.split(":");
        final Properties props = new Properties();
        props.setProperty("user", userPasswordSplit[0]);
        props.setProperty("password", userPasswordSplit[1]);
        final String hostDatabase = urlSplit[1];
        return DriverManager.getConnection("jdbc:postgresql://" + hostDatabase, props);
    }
}
