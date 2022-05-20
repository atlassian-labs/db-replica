package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.api.ThrottledCache;
import com.atlassian.db.replica.internal.MonotonicMemoryCache;
import com.atlassian.db.replica.internal.aurora.ReplicaNode;
import com.atlassian.db.replica.spi.Cache;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.time.Duration.ofSeconds;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
public class SequenceReplicaConsistency implements ReplicaConsistency {
    public static final Duration LSN_CHECK_LOCK_TIMEOUT = ofSeconds(1);

    private final AuroraSequence sequence;
    private final Cache<Long> lastWrite;
    private final boolean unknownWritesFallback;
    private final ConcurrentHashMap<String, SuppliedCache<Long>> multiReplicaLsnCache;
    private final ReplicaNode replicaNode;

    SequenceReplicaConsistency(
        String sequenceName,
        Cache<Long> lastWrite,
        boolean unknownWritesFallback,
        ConcurrentHashMap<String, SuppliedCache<Long>> multiReplicaLsnCache,
        ReplicaNode replicaNode
    ) {
        this.sequence = new AuroraSequence(sequenceName);
        this.lastWrite = lastWrite;
        this.unknownWritesFallback = unknownWritesFallback;
        this.multiReplicaLsnCache = multiReplicaLsnCache;
        this.replicaNode = replicaNode;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void write(Connection main) {
        try {
            long sequenceValue = sequence.fetch(main) + 1;
            lastWrite.put(sequenceValue);
            sequence.tryBump(main);
        } catch (Exception e) {
            throw new RuntimeException("Can't update consistency state.", e);
        }
    }

    @Override
    public boolean isConsistent(Database replica) {
        try {
            return lastWrite.get()
                .map(lastWrite1 -> computeReplicasConsistency(replica, lastWrite1))
                .orElse(unknownWritesFallback);
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred during consistency checking.", e);
        }
    }

    private boolean computeReplicasConsistency(Database replica, long lastWrite) {
        tryRefreshLsnCacheForCurrentReplica(replica);
        return isConsistent(replica, lastWrite);
    }

    private void tryRefreshLsnCacheForCurrentReplica(Database replica) {
        replica.getId().ifPresent(replicaId -> {
            multiReplicaLsnCache.computeIfAbsent(
                    replicaId,
                    x ->  ThrottledCache.<Long>builder(Clock.systemUTC(), LSN_CHECK_LOCK_TIMEOUT).build()
                )
                .get(() -> sequence.fetch(replica.getDataSource().getConnection()));

        });
    }

    private boolean isConsistent(Database replica, long lastWrite) {
        final Long lsn = multiReplicaLsnCache.get(replica.getId().orElse(null)).get().orElse(0L);
        return lsn >= lastWrite;
    }

    public static class Builder {
        private String sequenceName;
        private Cache<Long> lastWrite = new MonotonicMemoryCache<>();
        private boolean unknownWritesFallback = false;
        private ConcurrentHashMap<String, SuppliedCache<Long>> multiReplicaLsnCache = new ConcurrentHashMap<>();
        private ReplicaNode replicaNode = new ReplicaNode();

        public Builder sequenceName(String sequenceName) {
            this.sequenceName = sequenceName;
            return this;
        }

        public Builder lastWrite(Cache<Long> lastWrite) {
            this.lastWrite = lastWrite;
            return this;
        }

        public Builder unknownWritesFallback(boolean unknownWritesFallback) {
            this.unknownWritesFallback = unknownWritesFallback;
            return this;
        }

        public Builder multiReplicaLsnCache(ConcurrentHashMap<String, SuppliedCache<Long>> multiReplicaLsnCache) {
            this.multiReplicaLsnCache = multiReplicaLsnCache;
            return this;
        }

        public Builder replicaNode(ReplicaNode replicaNode) {
            this.replicaNode = replicaNode;
            return this;
        }

        /**
         * @deprecated use {@link Builder#sequenceName(String)}{@code .}{@link Builder#build()} instead.
         */
        @Deprecated
        public SequenceReplicaConsistency build(String sequenceName) {
            return sequenceName(sequenceName).build();
        }

        public SequenceReplicaConsistency build() {
            return new SequenceReplicaConsistency(
                sequenceName,
                lastWrite,
                unknownWritesFallback,
                multiReplicaLsnCache,
                replicaNode
            );
        }
    }
}
