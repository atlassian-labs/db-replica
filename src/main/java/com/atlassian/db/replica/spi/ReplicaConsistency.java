package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.internal.util.ThreadSafe;

import java.sql.Connection;
import java.util.function.Supplier;

/**
 * Tracks data consistency between replica and main databases.
 */
@ThreadSafe
public interface ReplicaConsistency {

    /**
     * Informs that {@code main} received an UPDATE, INSERT or DELETE or transaction commit
     * when in a transaction.
     *
     * @param main connects to the main database
     */
    void write(Connection main);

    /**
     * Invoked just before transaction commit.
     * <p>
     * Notice: The method will not handle all writes. Writes done outside of a transaction
     * needs to be handled in `ReplicaConnection#write`.
     *
     * @param main connects to the main database
     */
    default void preCommit(Connection main) {
    }

    /**
     * Judges if {@code replica} is ready to be queried.
     *
     * @param database returns a supplier for replica database
     * @return true if {@code replica} is consistent with main
     */
    boolean isConsistent(Database database);
}
