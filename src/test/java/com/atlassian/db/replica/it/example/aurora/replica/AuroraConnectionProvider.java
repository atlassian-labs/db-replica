package com.atlassian.db.replica.it.example.aurora.replica;

import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class AuroraConnectionProvider implements ConnectionProvider {
    private final String readerUrl;
    private final String writerUrl;
    private final String password;

    public AuroraConnectionProvider(String readerUrl, String writerUrl, String password) {
        this.readerUrl = readerUrl;
        this.writerUrl = writerUrl;
        this.password = password;
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
        return DriverManager.getConnection(
            url,
            "postgres",
            password
        );
    }
}
