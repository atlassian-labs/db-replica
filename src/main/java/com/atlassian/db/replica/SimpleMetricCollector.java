package com.atlassian.db.replica;

import com.atlassian.diagnostics.internal.platform.monitor.db.SqlOperation;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This collector will be used to collect metrics during query execution.
 * Some ideas what can it provide:
 * - read replica DB time / all DB time ratio
 * - read only paths
 * - ...
 */
public class SimpleMetricCollector {
    private final static AtomicInteger counter = new AtomicInteger();
    private final static AtomicLong replicaDuration = new AtomicLong();
    private final static AtomicLong writeDuration = new AtomicLong();

    public static <T> T measure(boolean isReadReplica, final SqlOperation<T> operation) throws SQLException {
        if (isReadReplica) {
            return executeOnReplica(operation);
        } else {
            return executeOnMain(operation);
        }
    }

    private static <T> T executeOnReplica(final SqlOperation<T> operation) throws SQLException {
        return execute(replicaDuration, operation);
    }

    private static <T> T executeOnMain(final SqlOperation<T> operation) throws SQLException {
        return execute(writeDuration, operation);
    }

    private static <T> T execute(AtomicLong duration, final SqlOperation<T> operation) throws SQLException {
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
