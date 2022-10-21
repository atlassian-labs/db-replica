package com.atlassian.db.replica.internal.observability.logs;

import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.util.function.Supplier;

import static java.lang.String.format;

public final class ReplicaConsistencyLogger implements ReplicaConsistency {
    private final ReplicaConsistency delegate;
    private final LazyLogger logger;

    public ReplicaConsistencyLogger(ReplicaConsistency delegate, LazyLogger logger) {
        this.delegate = delegate;
        this.logger = logger;
    }

    @Override
    public void write(Connection main) {
        try {
            delegate.write(main);
            logger.debug(() -> format("ReplicaConsistency#write(connection=%s)", main));
        } catch (Exception e) {
            logger.debug(() -> format("Failed ReplicaConsistency#write(connection=%s)", main), e);
            throw e;
        }
    }

    @Override
    public void preCommit(Connection main) {
        try {
            delegate.preCommit(main);
            logger.debug(() -> format("ReplicaConsistency#preCommit(connection=%s)", main));
        } catch (Exception e) {
            logger.debug(() -> format("Failed ReplicaConsistency#preCommit(connection=%s)", main), e);
            throw e;
        }
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        try {
            final boolean consistent = delegate.isConsistent(replica);
            logger.debug(() -> format("ReplicaConsistency#isConsistent = %b", consistent));
            return consistent;
        } catch (Exception e) {
            logger.debug(() -> "Failed ReplicaConsistency#isConsistent", e);
            throw e;
        }
    }
}
