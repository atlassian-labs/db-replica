package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.internal.util.ThreadSafe;

import java.sql.Connection;
import java.util.function.Supplier;

/**
 * Tracks data consistency between replica and main databases.
 */
@ThreadSafe
public interface ReplicaConsistency {

    /**
     * Informs that {@code main} received an UPDATE, INSERT or DELETE.
     *
     * @param main connects to the main database
     */
    void write(Connection main);

    /**
     * Judges if {@code replica} is ready to be queried.
     *
     * @param replica connects to the replica database
     * @return true if {@code replica} is consistent with main
     */
    boolean isConsistent(Supplier<Connection> replica);
}
