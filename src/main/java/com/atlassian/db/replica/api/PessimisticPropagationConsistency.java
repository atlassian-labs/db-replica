package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.MonotonicMemoryCache;
import com.atlassian.db.replica.spi.Cache;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Assumes that writes propagate from main to replicas in at most a given amount of time.
 * If it cannot remember the time of last write, pessimistically assumes it's going to be inconsistent.
 */
public final class PessimisticPropagationConsistency implements ReplicaConsistency {

    private final Clock clock;
    private final Duration maxPropagation;
    private final Cache<Instant> lastWrite;

    public static class Builder {
        private Duration maxPropagation = Duration.ofMillis(100);
        private Cache<Instant> lastWrite = new MonotonicMemoryCache<>();
        private Clock clock = Clock.systemUTC();

        /**
         * @param maxPropagation how long do writes propagate from main to replica
         */
        public Builder assumeMaxPropagation(Duration maxPropagation) {
            this.maxPropagation = maxPropagation;
            return this;
        }

        /**
         * @param lastWrite remembers last write
         */
        public Builder cacheLastWrite(Cache<Instant> lastWrite) {
            this.lastWrite = lastWrite;
            return this;
        }

        /**
         * @param clock measures flow of time
         */
        public Builder measureTime(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * @return consistency assuming consistency after max propagation since last write (if known)
         */
        public ReplicaConsistency build() {
            return new PessimisticPropagationConsistency(clock, maxPropagation, lastWrite);
        }
    }

    private PessimisticPropagationConsistency(Clock clock, Duration maxPropagation, Cache<Instant> lastWrite) {
        this.clock = clock;
        this.maxPropagation = maxPropagation;
        this.lastWrite = lastWrite;
    }

    @Override
    public void write(Connection main) {
        lastWrite.put(clock.instant());
    }

    @Override
    public boolean isConsistent(Database replica) {
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
     * <p>
     * Propagates write assumption to the cache. This prevents from assuming write just happened until the next write.
     *
     * @return known or assumed time of last write
     */
    private Instant assumeLastWrite() {
        return lastWrite
            .get()
            .orElseGet(this::assumeWriteJustHappened);
    }

    private Instant assumeWriteJustHappened() {
        final Instant now = clock.instant();
        lastWrite.put(now);
        return now;
    }
}
