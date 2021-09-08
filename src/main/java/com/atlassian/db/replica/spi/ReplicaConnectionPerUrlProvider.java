package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.jdbc.JdbcUrl;

@FunctionalInterface
public interface ReplicaConnectionPerUrlProvider {
    ReplicaConnectionProvider getReplicaConnectionProvider(JdbcUrl replicaUrl);
}
