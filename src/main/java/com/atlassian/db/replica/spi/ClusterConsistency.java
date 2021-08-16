package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.Database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;

public interface ClusterConsistency {

    /**
     * Informs that {@code main} received an UPDATE, INSERT or DELETE or transaction commit
     * when in a transaction.
     *
     * @param main connects to the main database
     */
    void write(Connection main) throws SQLException;

    /**
     * Invoked just before transaction commit.
     * <p>
     * Notice: The method will not handle all writes. Writes done outside of a transaction
     * needs to be handled in `ReplicaConnection#write`.
     *
     * @param main connects to the main database
     */
    default void preCommit(Connection main) throws SQLException {
    }

    /**
     * Provide consistent replica.
     *
     * @return true if {@code replica} is consistent with main
     */
    Optional<Database> getConsistent(Collection<Database> replicas) throws SQLException;
}
