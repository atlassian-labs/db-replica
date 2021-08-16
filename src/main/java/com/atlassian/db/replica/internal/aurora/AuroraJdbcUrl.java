package com.atlassian.db.replica.internal.aurora;

public class AuroraJdbcUrl {
    private static final String prefix = "jdbc:postgresql://";
    private final AuroraEndpoint endpoint;
    private final String databaseName;

    public AuroraJdbcUrl(AuroraEndpoint endpoint, String databaseName) {
        this.endpoint = endpoint;
        this.databaseName = databaseName;
    }

    public AuroraEndpoint getEndpoint() {
        return endpoint;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return String.format("%s%s/%s", prefix, endpoint, databaseName);
    }
}
