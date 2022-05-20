package com.atlassian.db.replica.api;

import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;

public class ReplicaConsistencyMock implements ReplicaConsistency {
    private final boolean consistent;

    public ReplicaConsistencyMock(boolean consistent) {
        this.consistent = consistent;
    }

    @Override
    public void write(Connection main) {

    }

    @Override
    public boolean isConsistent(Database replica) {
        Connection connection = replica.getDataSource().getConnection();
        return consistent;
    }
}
