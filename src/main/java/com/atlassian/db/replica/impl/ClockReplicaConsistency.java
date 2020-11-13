package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.spi.*;
import net.jcip.annotations.*;

import java.sql.*;
import java.time.*;
import java.util.concurrent.atomic.*;

import static org.apache.commons.lang3.ObjectUtils.*;

/**
 * Assumes replica is up to date after a specified time.
 */
@ThreadSafe
public class ClockReplicaConsistency implements ReplicaConsistency {

    /**
     * Estimated replica refresh time.
     */
    private static final Duration REFRESHING = Duration.ofMillis(100);

    private final Clock clock;
    private final AtomicReference<Instant> lastWrite;

    public ClockReplicaConsistency(Clock clock) {
        this.clock = clock;
        lastWrite = new AtomicReference<>(clock.instant());
    }

    @Override
    public void write(Connection main) {
        Instant now = clock.instant();
        lastWrite.updateAndGet(prev -> max(prev, now));
    }

    @Override
    public boolean isConsistent(Connection replica) {
        Instant lastRefresh = clock.instant().minus(REFRESHING);
        return lastWrite.get().isBefore(lastRefresh);
    }
}
