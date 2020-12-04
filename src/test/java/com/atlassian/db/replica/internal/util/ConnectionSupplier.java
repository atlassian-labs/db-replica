package com.atlassian.db.replica.internal.util;

import java.sql.Connection;
import java.util.function.Supplier;

public class ConnectionSupplier implements Supplier<Connection> {
    private final Connection connection;

    public ConnectionSupplier(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection get() {
        return connection;
    }
}
