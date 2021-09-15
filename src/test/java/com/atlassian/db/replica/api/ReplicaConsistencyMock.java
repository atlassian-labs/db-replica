package com.atlassian.db.replica.api;

import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.util.function.Supplier;

public class ReplicaConsistencyMock implements ReplicaConsistency {
    private final boolean consistent;

    public ReplicaConsistencyMock(boolean consistent) {
        this.consistent = consistent;
    }

    @Override
    public void write(Connection main) {

    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        Connection connection = replica.get();
        return consistent;
    }
}
