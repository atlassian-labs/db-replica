package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.DualCall;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class TimeRatioPrinter implements DualCall {
    private final static AtomicInteger counter = new AtomicInteger();
    private final static AtomicLong replicaDuration = new AtomicLong();
    private final static AtomicLong writeDuration = new AtomicLong();

    @Override
    public <T> T callReplica(final SqlCall<T> call) throws SQLException {
        return call(replicaDuration, call);
    }

    @Override
    public <T> T callMain(final SqlCall<T> call) throws SQLException {
        return call(writeDuration, call);
    }

    private <T> T call(AtomicLong duration, final SqlCall<T> call) throws SQLException {
        final long start = System.currentTimeMillis();
        final T returnValue = call.call();
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
