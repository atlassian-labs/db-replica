package com.atlassian.db.replica.api;

import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.util.function.Supplier;

public class TransactionReuse implements ReplicaConsistency {

    private final ReplicaConsistency delegate;

    public TransactionReuse(ReplicaConsistency delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(Connection main) {
        try {
            if (main.getAutoCommit()) {
                delegate.write(main);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preCommit(Connection main) {
        delegate.write(main);
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        return delegate.isConsistent(replica);
    }
}
