package com.atlassian.db.replica.api;

import java.sql.Connection;
import java.util.function.Supplier;

public interface Database {
    String getId();

    Supplier<Connection> getConnectionSupplier();
}
