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
