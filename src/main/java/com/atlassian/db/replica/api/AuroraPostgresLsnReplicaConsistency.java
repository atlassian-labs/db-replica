package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.MonotonicMemoryCache;
import com.atlassian.db.replica.spi.Cache;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    public static final class Builder {
        private Cache<Long> lastWrite = new MonotonicMemoryCache<>();

        /**
         * @param lastWrite remembers last write
         */
        public Builder cacheLastWrite(Cache<Long> lastWrite) {
            this.lastWrite = lastWrite;
            return this;
        }

        /**
         * @return consistency assuming that LSN (log sequence number) is greater or equal to LSN for last write (if known)
         */
        public AuroraPostgresLsnReplicaConsistency build() {
            return new AuroraPostgresLsnReplicaConsistency(lastWrite);
        }
    }

    private AuroraPostgresLsnReplicaConsistency(Cache<Long> lastWrite) {
        this.lastWrite = lastWrite;
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
                .map(lastWriteLsn -> queryReplicaDbLsn(replica.get()) >= lastWriteLsn)
                .orElse(false);
        } catch (Exception e) {
            return false;
        }
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
