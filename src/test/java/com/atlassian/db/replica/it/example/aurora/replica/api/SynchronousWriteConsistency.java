package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.internal.LazyReference;
import com.atlassian.db.replica.it.example.aurora.replica.spi.Latency;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Waits until writes propagate to replica.
 * It doesn't depend on a cross-JVM cache.
 */

// TODO add also DeferredSynchronousWriteConsistency implementation
// TODO multi replica support
public class SynchronousWriteConsistency implements ReplicaConsistency {
    private final ReplicaConsistency replicaConsistency;
    private final ConnectionProvider connections;
    private final Latency latency;

    /**
     * @param replicaConsistency checks consistency
     * @param connections        connects to replica during polling
     * @param latency
     */
    public SynchronousWriteConsistency(
        ReplicaConsistency replicaConsistency,
        ConnectionProvider connections,
        Latency latency
    ) {
        this.replicaConsistency = replicaConsistency;
        this.connections = connections;
        this.latency = latency;
    }

    @Override
    public void write(Connection connection) {
        replicaConsistency.write(connection);
        Waiting waiting = new Waiting(replicaConsistency, connections);
        waiting.waitUntilConsistent(latency);
    }

    @Override
    public boolean isConsistent(Supplier<Connection> supplier) {
        return replicaConsistency.isConsistent(supplier);
    }

    public static class Waiting {
        private final ReplicaConsistency consistency;
        private final ConnectionProvider connections;

        public Waiting(
            ReplicaConsistency consistency,
            ConnectionProvider connections
        ) {
            this.consistency = consistency;
            this.connections = connections;
        }

        public void waitUntilConsistent(Latency latency) {
            try {
                waitUntilConsistent(getTimeout(), latency);
            } catch (Exception exception) {
                throw new RuntimeException("TODO", exception);
            }
        }

        private void waitUntilConsistent(Duration timeout, Latency latency) throws Exception {
            final Instant end = Instant.now().plus(Duration.ofMillis(adjustTimeout(timeout)));
            while (Instant.now().isBefore(end)) {
                final boolean isConsistent = checkConsistency();// TODO: Measure `Latency`
                if (isConsistent) {
                    // TODO increment success counter?
                    return;
                }
                Thread.sleep(10);
            }
            // statsDClient.increment(WAIT_UNTIL_CONSISTENT_FAILURE);
            // throw new WaitOnWriteConsistencyException("Waiting for consistency failed: " + timeout, e);
            throw new RuntimeException("TODO");
        }

        private long adjustTimeout(Duration timeout) {
            final Duration maxTimeout = getTimeout();
            return max(min(timeout.toMillis(), maxTimeout.toMillis()), 10);
        }

        private boolean checkConsistency() {
            try (ConnectionSupplier replica = new ConnectionSupplier(connections)) {
                return consistency.isConsistent(replica);
            } catch (Exception e) {
                throw new RuntimeException("TODO", e);
//                throw new WaitOnWriteConsistencyException("Checking consistency failed: " + consistency, e);
            }
        }

        private Duration getTimeout() {
            return Duration.ofSeconds(30); // TODO: parametrise
        }

    }

    private static class ConnectionSupplier implements Supplier<Connection>, AutoCloseable {
        private final LazyReference<Connection> connection;

        private ConnectionSupplier(ConnectionProvider connectionProvider) {
            connection = new LazyReference<Connection>() {
                @Override
                protected Connection create() throws Exception {
                    return connectionProvider.getReplicaConnection();
                }
            };
        }

        @Override
        public void close() throws Exception {
            if (connection.isInitialized()) {
                final Connection connection = this.connection.get();
                if (connection != null) {
                    connection.close();
                }
            }
        }

        @Override
        public Connection get() {
            return connection.get();
        }
    }

}
