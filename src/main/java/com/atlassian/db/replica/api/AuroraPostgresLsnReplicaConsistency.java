package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.MonotonicMemoryCache;
import com.atlassian.db.replica.internal.NoCacheSuppliedCache;
import com.atlassian.db.replica.spi.Cache;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * It remembers last write LSN (log sequence number) and compares it with LSN for subsequent reads to detect inconsistencies.
 * If it cannot remember the LSN of last write, pessimistically assumes it's going to be inconsistent.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-monitoring.html">Monitoring Aurora PostgreSQL-based Aurora global databases</a>
 * <p>
 * It's an PostgreSQL-based Aurora database engine specific implementation.
 */
public final class AuroraPostgresLsnReplicaConsistency implements ReplicaConsistency {

    private final Cache<Long> lastWrite;
    private final SuppliedCache<Long> replicaLsnCache;

    public static final class Builder {
        private Cache<Long> lastWrite = new MonotonicMemoryCache<>();
        private SuppliedCache<Long> replicaLsnCache = new NoCacheSuppliedCache<>();

        /**
         * @param lastWrite remembers last write
         */
        public Builder cacheLastWrite(Cache<Long> lastWrite) {
            this.lastWrite = lastWrite;
            return this;
        }

        /**
         * @param cache remembers replica lsn (potentially stale)
         */
        public Builder replicaLsnCache(SuppliedCache<Long> cache) {
            this.replicaLsnCache = cache;
            return this;
        }

        /**
         * @return consistency assuming that LSN (log sequence number) is greater or equal to LSN for last write (if known)
         */
        public AuroraPostgresLsnReplicaConsistency build() {
            return new AuroraPostgresLsnReplicaConsistency(lastWrite, replicaLsnCache);
        }
    }

    private AuroraPostgresLsnReplicaConsistency(
        Cache<Long> lastWrite,
        SuppliedCache<Long> replicaLsnCache
    ) {
        this.lastWrite = lastWrite;
        this.replicaLsnCache = replicaLsnCache;
    }

    @Override
    public void write(Connection main) {
        try {
            lastWrite.put(queryMainDbLsn(main));
        } catch (Exception e) {
            throw new RuntimeException("failure during LSN fetching for main database", e);
        }
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        try {
            return lastWrite.get()
                .flatMap(lastWriteLsn -> isConsistentBasedOnLsn(replica, lastWriteLsn))
                .orElse(false);
        } catch (Exception e) {
            return false;
        }
    }

    private Optional<Boolean> isConsistentBasedOnLsn(Supplier<Connection> replica, Long lastWriteLsn) {
        return replicaLsnCache
            .get(() -> queryReplicaDbLsn(replica.get()))
            .map(lsn -> lsn >= lastWriteLsn);
    }

    /**
     * @return LSN (log sequence number) for the main database (the highest LSN)
     */
    private long queryMainDbLsn(Connection connection) {
        return queryLsn(connection, "SELECT MAX(durable_lsn) AS lsn FROM aurora_global_db_instance_status();");
    }

    /**
     * @return LSN (log sequence number) for the most outdated replica database (the lowest LSN)
     */
    private long queryReplicaDbLsn(Connection connection) {
        return queryLsn(connection, "SELECT MIN(durable_lsn) AS lsn FROM aurora_global_db_instance_status();");
    }

    private long queryLsn(Connection connection, String rawSqlQuery) {
        try (
            PreparedStatement query = connection.prepareStatement(rawSqlQuery);
            ResultSet results = query.executeQuery()
        ) {
            results.next();
            return results.getLong("lsn");
        } catch (SQLException e) {
            throw new RuntimeException("An SQLException occurred during LSN fetching", e);
        }
    }

}
