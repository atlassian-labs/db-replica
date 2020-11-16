package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.api.SqlOperation;
import com.atlassian.db.replica.spi.DualConnectionOperation;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class PrintfDualConnectionOperation implements DualConnectionOperation {
    private final static AtomicInteger counter = new AtomicInteger();
    private final static AtomicLong replicaDuration = new AtomicLong();
    private final static AtomicLong writeDuration = new AtomicLong();

    @Override
    public <T> T executeOnReplica(final SqlOperation<T> operation) throws SQLException {
        return execute(replicaDuration, operation);
    }

    @Override
    public <T> T executeOnMain(final SqlOperation<T> operation) throws SQLException {
        return execute(writeDuration, operation);
    }

    private <T> T execute(AtomicLong duration, final SqlOperation<T> operation) throws SQLException {
        final long start = System.currentTimeMillis();
        final T returnValue = operation.execute();
        duration.addAndGet(System.currentTimeMillis() - start);
        if (counter.incrementAndGet() % 100 == 0) {
            final long main = writeDuration.get();
            final long replica = replicaDuration.get();
            final String message = String.format(
                "===> main/replica/ratio: %s/%s/%s",
                Duration.ofMillis(main),
                Duration.ofMillis(replica),
                String.format("%.2f %%", replica / (main + replica + 0.0) * 100)
            );
            System.out.println(message);
        }
        return returnValue;
    }
}
