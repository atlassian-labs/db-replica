package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.TransactionHook;

import java.sql.Connection;

public class ConsistencyToTransactionHookAdapter implements TransactionHook {
    private final ReplicaConsistency replicaConsistency;

    public ConsistencyToTransactionHookAdapter(ReplicaConsistency replicaConsistency) {
        this.replicaConsistency = replicaConsistency;
    }

    @Override
    public void write(Connection main) {
        replicaConsistency.write(main);
    }

    @Override
    public void preCommit(Connection main) {
        replicaConsistency.preCommit(main);
    }
}
