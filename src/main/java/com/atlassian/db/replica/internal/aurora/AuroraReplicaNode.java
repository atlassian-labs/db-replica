package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.api.AuroraConnectionDetails;
import com.atlassian.db.replica.internal.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
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
                final String url = "postgresql://" + auroraConnectionDetails.getUsername() + ":" + auroraConnectionDetails.getPassword() + "@" + auroraUrl.getEndpoint() + "/" + auroraUrl.getDatabaseName();
                return replicaNode.mark(
                    getConnection(url),
                    auroraUrl.getEndpoint().getServerId()
                );
            } catch (SQLException exception) {
                throw new ReadReplicaConnectionCreationException(exception);
            }
        };
    }

    private Connection getConnection(String url) throws SQLException {
        final PostgresUri postgresUri = new PostgresUri(url);
        final Properties props = new Properties();
        props.setProperty("user", postgresUri.getUser());
        props.setProperty("password", postgresUri.getPassword());
        return DriverManager.getConnection(postgresUri.getJdbcUrl(), props);
    }

    public static class PostgresUri {
        private final String password;
        private final String user;
        private final String jdbcUrl;

        public PostgresUri(String url) {
            final String[] urlSplit = url.replace("postgresql://", "").split("@");
            final String userPassword = urlSplit[0];
            final String[] userPasswordSplit = userPassword.split(":");
            this.user = userPasswordSplit[0];
            this.password = userPasswordSplit[1];
            final String hostDatabase = urlSplit[1];
            this.jdbcUrl = "jdbc:postgresql://" + hostDatabase;
        }

        public String getPassword() {
            return password;
        }

        public String getUser() {
            return user;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PostgresUri that = (PostgresUri) o;
            return Objects.equals(password, that.password) && Objects.equals(user, that.user) && Objects.equals(
                jdbcUrl,
                that.jdbcUrl
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(password, user, jdbcUrl);
        }

        @Override
        public String toString() {
            return "PostgresUri{" +
                "password='" + password + '\'' +
                ", user='" + user + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                '}';
        }
    }
}
