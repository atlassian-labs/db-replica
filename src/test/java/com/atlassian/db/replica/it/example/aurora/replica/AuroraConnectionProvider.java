package com.atlassian.db.replica.it.example.aurora.replica;

import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public final class AuroraConnectionProvider implements ConnectionProvider {
    private final String readerUrl;
    private final String writerUrl;

    public AuroraConnectionProvider(String readerUrl, String writerUrl) {
        this.readerUrl = readerUrl;
        this.writerUrl = writerUrl;
    }

    @Override
    public boolean isReplicaAvailable() {
        return true;
    }

    @Override
    public Connection getMainConnection() throws SQLException {
        return getConnection(writerUrl);
    }

    @Override
    public Connection getReplicaConnection() throws SQLException {
        return getConnection(readerUrl);
    }

    private Connection getConnection(String url) throws SQLException {
        final Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", System.getenv("password"));
        return DriverManager.getConnection(url, props);
    }
}
