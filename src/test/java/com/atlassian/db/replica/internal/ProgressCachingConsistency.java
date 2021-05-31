package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.Cache;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Compares the progress of main and replica databases.
 * Caches the last progress of main and replica databases instead of querying them every time.
 * Falls back if any cache is empty.
 */
public class ProgressCachingConsistency<T extends Comparable<T>> implements ReplicaConsistency {

    private final Cache<T> lastMain;
    private final SuppliedCache<T> lastReplica;
    private final DatabaseProgress<T> progress;
    private final boolean fallback;

    public static class Builder<T extends Comparable<T>> {

        private DatabaseProgress<T> progress;
        private Cache<T> lastMain = Cache.cacheMonotonicValuesInMemory();
        private SuppliedCache<T> lastReplica = new NoCacheSuppliedCache<>();
        private boolean fallback = false;

        public Builder(DatabaseProgress<T> progress) {
            this.progress = progress;
        }

        /**
         * @param progress tracks the current version of database contents
         */
        public Builder<T> progress(DatabaseProgress<T> progress) {
            this.progress = progress;
            return this;
        }

        /**
         * @param lastMain caches the last known progress of the main database
         */
        public Builder<T> lastMain(Cache<T> lastMain) {
            this.lastMain = lastMain;
            return this;
        }

        /**
         * @param lastReplica caches the last known progress of the replica database
         */
        public Builder<T> lastReplica(SuppliedCache<T> lastReplica) {
            this.lastReplica = lastReplica;
            return this;
        }

        /**
         * @return adds optimism to the consistency
         */
        public Builder<T> fallBackToConsistency() {
            this.fallback = true;
            return this;
        }

        /**
         * @return adds optimism to the consistency
         */
        public Builder<T> fallBackToInconsistency() {
            this.fallback = false;
            return this;
        }

        public ReplicaConsistency build() {
            return new ProgressCachingConsistency<>(progress, lastMain, lastReplica, fallback);
        }
    }

    private ProgressCachingConsistency(
        DatabaseProgress<T> progress,
        Cache<T> lastMain,
        SuppliedCache<T> lastReplica,
        boolean fallback
    ) {
        this.lastMain = lastMain;
        this.progress = progress;
        this.lastReplica = lastReplica;
        this.fallback = fallback;
    }

    @Override
    public void write(Connection main) {
        T mainProgress = progress.updateMain(() -> main);
        lastMain.put(mainProgress);
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        return lastMain
            .get()
            .flatMap(mainProgress -> consistentIfReplicaCaughtUp(replica, mainProgress))
            .orElse(fallback);
    }

    private Optional<Boolean> consistentIfReplicaCaughtUp(Supplier<Connection> replica, T mainProgress) {
        return lastReplica
            .get(() -> progress.getReplica(replica))
            .map(replicaProgress -> replicaProgress.compareTo(mainProgress) >= 0);
    }
}
