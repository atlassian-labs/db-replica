package com.atlassian.db.replica.internal.util;

import com.atlassian.db.replica.spi.DataSource;

import java.sql.Connection;
import java.util.function.Supplier;

public class ConnectionSupplier implements DataSource {
    private final Connection connection;

    public ConnectionSupplier(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }
}
