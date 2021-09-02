package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.jdbc.JdbcUrl;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.ReplicaConnectionProvider;

import java.sql.DriverManager;

public class DefaultReplicaConnectionPerUrlProvider implements ReplicaConnectionPerUrlProvider {
    private final String username;
    private final String password;

    public DefaultReplicaConnectionPerUrlProvider(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public ReplicaConnectionProvider getReplicaConnectionProvider(JdbcUrl replicaUrl) {
        return () -> DriverManager.getConnection(replicaUrl.toString(), username, password);
    }
}
