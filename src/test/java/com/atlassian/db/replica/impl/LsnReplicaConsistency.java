package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.spi.ReplicaConsistency;
import net.jcip.annotations.ThreadSafe;
import org.postgresql.replication.LogSequenceNumber;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicReference;

import static com.atlassian.db.replica.internal.util.Comparables.max;

/**
 * [LSN] means "log sequence number". It points to a place in the PostgreSQL write-ahead log.
 * Each replica updates its WAL via recovery. Recovery-time LSN can be queried by recovery info.
 * When a replica is caught up, it's no longer in recovery. Non-recovery LSN can be queried by backup control.
 *
 * @see <a href="https://www.postgresql.org/docs/9.6/datatype-pg-lsn.html">LSN</a>
 * @see <a href="https://www.postgresql.org/docs/9.6/functions-admin.html#FUNCTIONS-RECOVERY-INFO-TABLE">recovery info</a>
 * @see <a href="https://www.postgresql.org/docs/9.6/functions-admin.html#FUNCTIONS-ADMIN-BACKUP-TABLE">backup control</a>
 *
 * It's a DB specific implementation used in integration tests.
 */
@ThreadSafe
public class LsnReplicaConsistency implements ReplicaConsistency {

    private final AtomicReference<LogSequenceNumber> lastWrite = new AtomicReference<>();

    @Override
    public void write(Connection main) {
        try {
            final LogSequenceNumber next = queryLsn(main);
            lastWrite.updateAndGet(prev -> max(prev, next));
        } catch (Exception e) {
            //TODO: log warning
            lastWrite.set(null);
        }
    }

    @Override
    public boolean isConsistent(Connection replica) {
        if (lastWrite.get() == null) {
            return false;
        }
        LogSequenceNumber lastRefresh;
        try {
            lastRefresh = queryLsn(replica);
        } catch (Exception e) {
            //TODO: log warning
            return false;
        }
        return lastRefresh.asLong() >= lastWrite.get().asLong();
    }

    private LogSequenceNumber queryLsn(Connection connection) throws Exception {
        try (
            PreparedStatement query = prepareQuery(connection);
            ResultSet results = query.executeQuery()
        ) {
            results.next();
            String lsn = results.getString("lsn");
            return LogSequenceNumber.valueOf(lsn);
        }
    }

    private PreparedStatement prepareQuery(Connection connection) throws Exception {
        return connection.prepareStatement(
            "SELECT\n" +
                "CASE WHEN pg_is_in_recovery()\n" +
                "   THEN pg_last_xlog_replay_location()\n" +
                "   ELSE pg_current_xlog_location()\n" +
                "END AS lsn;"
        );
    }
}
