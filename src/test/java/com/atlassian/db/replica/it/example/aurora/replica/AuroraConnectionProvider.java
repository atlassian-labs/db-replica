package com.atlassian.db.replica.it.example.aurora.replica;

import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public final class AuroraConnectionProvider implements ConnectionProvider {
    private final String readerEndpoint;
    private final String writerEndpoint;

    public AuroraConnectionProvider(String readerEndpoint, String writerEndpoint) {
        this.readerEndpoint = readerEndpoint;
        this.writerEndpoint = writerEndpoint;
    }

    @Override
    public boolean isReplicaAvailable() {
        return true;
    }

    @Override
    public Connection getMainConnection() throws SQLException {
        return getConnection(writerEndpoint);
    }

    @Override
    public Connection getReplicaConnection() throws SQLException {
        return getConnection(readerEndpoint);
    }

    private Connection getConnection(String url) throws SQLException {
        final Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", System.getenv("password"));
        return DriverManager.getConnection(url, props);
    }
}
