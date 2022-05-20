package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.internal.LazyReference;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.DataSource;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * Waits until writes propagate to replica.
 * It doesn't depend on a cross-JVM cache.
 */

public class SynchronousWriteConsistency implements ReplicaConsistency {
    private final ReplicaConsistency replicaConsistency;
    private final ConnectionProvider connections;

    /**
     * @param replicaConsistency checks consistency
     * @param connections        connects to replica during polling
     */
    public SynchronousWriteConsistency(
        ReplicaConsistency replicaConsistency,
        ConnectionProvider connections
    ) {
        this.replicaConsistency = replicaConsistency;
        this.connections = connections;
    }

    @Override
    public void write(Connection connection) {
        replicaConsistency.write(connection);
        Waiting waiting = new Waiting(replicaConsistency, connections);
        waiting.waitUntilConsistent();
    }

    @Override
    public boolean isConsistent(Database database) {
        return replicaConsistency.isConsistent(database);
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

        public void waitUntilConsistent() {
            try {
                waitUntilConsistent(getTimeout());
            } catch (Exception exception) {
                throw new RuntimeException("TODO", exception);
            }
        }

        private void waitUntilConsistent(Duration timeout) throws Exception {
            final Instant end = Instant.now().plus(Duration.ofMillis(adjustTimeout(timeout)));
            while (Instant.now().isBefore(end)) {
                final boolean isConsistent = checkConsistency();
                if (isConsistent) {
                    return;
                }
                Thread.sleep(10);
            }
            throw new RuntimeException(format("Waiting for consistency failed: %s", timeout));
        }

        private long adjustTimeout(Duration timeout) {
            final Duration maxTimeout = getTimeout();
            return max(min(timeout.toMillis(), maxTimeout.toMillis()), 10);
        }

        private boolean checkConsistency() {
            try (LazyDataSource replica = new LazyDataSource(connections)) {
                return consistency.isConsistent(() -> replica);
            } catch (Exception e) {
                throw new RuntimeException(format("Checking consistency failed: %s", consistency), e);
            }
        }

        private Duration getTimeout() {
            return Duration.ofSeconds(30);
        }

    }

    private static class LazyDataSource implements DataSource, AutoCloseable {
        private final LazyReference<Connection> connection;

        private LazyDataSource(ConnectionProvider connectionProvider) {
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
        public Connection getConnection() {
            return connection.get();
        }
    }

}
