package com.atlassian.db.replica.internal;

import java.sql.Connection;
import java.util.function.Supplier;

public interface Database {
    String getId();

    Supplier<Connection> getConnectionSupplier();
}
