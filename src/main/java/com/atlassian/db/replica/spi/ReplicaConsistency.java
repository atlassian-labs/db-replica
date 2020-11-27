package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.internal.*;
import com.atlassian.db.replica.internal.util.*;

import java.sql.*;
import java.time.*;

@ThreadSafe
public interface ReplicaConsistency {

    /**
     * @param maxPropagation how long do writes propagate from main to replica
     * @param clock          measures flow of time
     * @param lastWrite      remembers last write
     * @return consistency checker assuming consistency after {@code maxPropagation} since {@code lastWrite} (if known)
     */
    static ReplicaConsistency assumePropagationDelay(Duration maxPropagation, Clock clock, Cache<Instant> lastWrite) {
        return new PessimisticPropagationConsistency(clock, maxPropagation, lastWrite);
    }

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
    boolean isConsistent(Connection replica);
}
