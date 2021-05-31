package com.atlassian.db.replica.internal;

import java.sql.Connection;
import java.util.function.Supplier;

public interface DatabaseProgress<T extends Comparable<T>> {

    T updateMain(Supplier<Connection> main);

    T getMain(Supplier<Connection> main);

    T getReplica(Supplier<Connection> replica);
}
