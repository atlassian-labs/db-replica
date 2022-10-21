package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.jdbc.JdbcProtocol;
import com.atlassian.db.replica.api.jdbc.JdbcUrl;

final class AuroraJdbcUrl {
    private static final String PREFIX = "jdbc:postgresql://";

    private final AuroraEndpoint endpoint;
    private final String databaseName;

    AuroraJdbcUrl(AuroraEndpoint endpoint, String databaseName) {
        this.endpoint = endpoint;
        this.databaseName = databaseName;
    }

    AuroraEndpoint getEndpoint() {
        return endpoint;
    }

    String getDatabaseName() {
        return databaseName;
    }

    JdbcUrl toJdbcUrl() {
        return JdbcUrl.builder()
            .protocol(JdbcProtocol.POSTGRES)
            .endpoint(endpoint.toString())
            .database(databaseName)
            .build();
    }

    @Override
    public String toString() {
        return String.format("%s%s/%s", PREFIX, endpoint, databaseName);
    }
}
