package com.atlassian.db.replica.spi;

import java.sql.Connection;

public interface TransactionHook {
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
     * needs to be handled in `TransactionHook#write`.
     *
     * @param main connects to the main database
     */
     void preCommit(Connection main);
}
