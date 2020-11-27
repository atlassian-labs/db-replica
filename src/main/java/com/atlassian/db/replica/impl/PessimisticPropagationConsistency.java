package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.spi.*;

import java.sql.*;
import java.time.*;

/**
 * Assumes that writes propagate from main to replicas in at most a given amount of time.
 * If it cannot remember the time of last write, pessimistically assumes it's going to be inconsistent.
 */
public class PessimisticPropagationConsistency implements ReplicaConsistency {

    private final Clock clock;
    private final Duration maxPropagation;
    private final Cache<Instant> lastWrite;

    public PessimisticPropagationConsistency(Clock clock, Duration maxPropagation, Cache<Instant> lastWrite) {
        this.clock = clock;
        this.maxPropagation = maxPropagation;
        this.lastWrite = lastWrite;
    }

    @Override
    public void write(Connection main) {
        lastWrite.put(clock.instant());
    }

    @Override
    public boolean isConsistent(Connection replica) {
        Instant assumedRefresh = assumeLastRefresh();
        Instant assumedWrite = assumeLastWrite();
        return assumedRefresh.isAfter(assumedWrite);
    }

    /**
     * @return assumed time of last replica refresh
     */
    private Instant assumeLastRefresh() {
        return clock.instant().minus(maxPropagation);
    }

    /**
     * If {@code lastWrite} is unknown, assume the write just happened, e.g. it didn't propagate yet.
     * This assumption errs on the side of caution: more true inconsistencies at the cost of fewer true consistencies.
     *
     * @return known or assumed time of last write
     */
    private Instant assumeLastWrite() {
        return lastWrite.get().orElse(clock.instant());
    }
}
