package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class CircularConsistency implements ReplicaConsistency {
    private final List<Boolean> consistency;
    private final boolean ignoreSupplier;
    private final AtomicInteger counter = new AtomicInteger();

    private CircularConsistency(List<Boolean> consistency, boolean ignoreSupplier) {
        this.consistency = consistency;
        this.ignoreSupplier = ignoreSupplier;
    }

    @Override
    public void write(Connection main) {

    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        if (!this.ignoreSupplier) {
            replica.get();
        }
        return consistency.get(counter.getAndIncrement() % consistency.size());
    }

    public static Builder permanentConsistency() {
        return new Builder(ImmutableList.of(true));
    }

    public static Builder permanentInconsistency() {
        return new Builder(ImmutableList.of(false));
    }

    public static class Builder {
        private final List<Boolean> consistency;
        private boolean ignoreSupplier = false;

        public Builder(List<Boolean> consistency) {
            this.consistency = consistency;
        }

        public Builder ignoreSupplier(boolean ignoreSupplier) {
            this.ignoreSupplier = ignoreSupplier;
            return this;
        }

        public ReplicaConsistency build() {
            return new CircularConsistency(consistency, ignoreSupplier);
        }
    }
}
