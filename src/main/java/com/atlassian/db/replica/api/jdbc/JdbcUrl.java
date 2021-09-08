package com.atlassian.db.replica.api.jdbc;

import java.net.URI;

public final class JdbcUrl {
    private static final String PREFIX = "jdbc";

    private final String internalUrl;

    private JdbcUrl(JdbcProtocol protocol, String endpoint, String database) {
        String url = String.format("%s:%s://%s/%s", PREFIX, protocol, endpoint, database);
        this.internalUrl = URI.create(url).toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return this.internalUrl;
    }

    public static final class Builder {
        private JdbcProtocol protocol;
        private String endpoint;
        private String database;

        private Builder() {
        }

        public Builder protocol(JdbcProtocol protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public JdbcUrl build() {
            return new JdbcUrl(protocol, endpoint, database);
        }
    }
}
