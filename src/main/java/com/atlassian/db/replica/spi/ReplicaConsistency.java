package com.atlassian.db.replica.spi;


import com.atlassian.db.replica.internal.util.ThreadSafe;

import java.sql.Connection;

@ThreadSafe
public interface ReplicaConsistency {
    /**
     * This method is called after each UPDATE, INSERT or DELETE.
     * TODO: Just an idea. If we pass a table that is updated to the listener. Then it can ignore bump for some writes.
     * @param main to the main database
     */
     void write(Connection main);

    /**
     * Judge if replica is ready to be queried.
     * @return true if replica is consistent with main
     */
     boolean isConsistent(Connection replica);
}
