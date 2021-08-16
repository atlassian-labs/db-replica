package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.api.ThrottledCache;
import com.atlassian.db.replica.internal.aurora.ReplicaNode;
import com.atlassian.db.replica.internal.MonotonicMemoryCache;
import com.atlassian.db.replica.spi.Cache;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.time.Duration.ofSeconds;

//TODO promote it to the API
//TODO add logs
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
public class SequenceReplicaConsistency implements ReplicaConsistency {
    public static final Duration LSN_CHECK_LOCK_TIMEOUT = ofSeconds(1); // TODO use a parameter instead of static

    private final AuroraSequence sequence;
    private final Cache<Long> lastWrite;
    private final boolean unknownWritesFallback;
    private final ConcurrentHashMap<String, SuppliedCache<Long>> multiReplicaLsnCache; //TODO: move to MultiReplicaConsistency
    // SequenceReplicaConsistency should know nothing about the sequence.
    private final ReplicaNode replicaNode;

    public static final class Builder {
        private Cache<Long> lastWrite = new MonotonicMemoryCache<>();
        private boolean unknownWritesFallback = false;
        private ConcurrentHashMap<String, SuppliedCache<Long>> lsnCache = new ConcurrentHashMap<>();
        private ReplicaNode replicaNode = new ReplicaNode();

        public Builder cacheLastWrite(Cache<Long> lastWrite) {
            this.lastWrite = lastWrite;
            return this;
        }

        public Builder multiReplicaLsnCache(ConcurrentHashMap<String, SuppliedCache<Long>> cache) {
            this.lsnCache = cache;
            return this;
        }

        public Builder assumeInconsistencyIfMainIsUnknown() {
            this.unknownWritesFallback = false;
            return this;
        }

        public Builder assumeConsistencyIfNoWritesWereObserved() {
            this.unknownWritesFallback = true;
            return this;
        }

        public Builder replicaNode(ReplicaNode replicaNode) {
            this.replicaNode = replicaNode;
            return this;
        }

        public SequenceReplicaConsistency build(String sequenceName) {
            return new SequenceReplicaConsistency(
                sequenceName,
                lastWrite,
                unknownWritesFallback,
                lsnCache,
                replicaNode
            );
        }
    }

    private SequenceReplicaConsistency(
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

    @Override
    public void write(Connection main) {
        try {
            long sequenceValue = sequence.fetch(main) + 1;
            lastWrite.put(sequenceValue);
            sequence.tryBump(main);
        } catch (Exception e) {
//            LOG.withoutCustomerData().warn(
//                "error occurred during putting sequence[{}] value into cache",
//                sequenceName,
//                e
//            );
            lastWrite.reset(); //TODO: is it ok to reset the cache for WAW?
        }
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        try {
            Optional<Long> lastWriteSequenceValue = lastWrite.get();
            return lastWriteSequenceValue
                .map(lastWrite1 -> computeReplicasConsistency(replica.get(), lastWrite1))
                .orElse(unknownWritesFallback);
        } catch (Exception e) {
//            LOG.withoutCustomerData().warn("exception occurred during consistency checking", e);
//            return false;
            throw new RuntimeException("TODO",e);
        }
    }

    private boolean computeReplicasConsistency(Connection replica, long lastWrite) {
        tryRefreshLsnCacheForCurrentReplica(replica);
        return isConsistent(replica, lastWrite);
    }

    private void tryRefreshLsnCacheForCurrentReplica(Connection replica) {
        String replicaId = replicaNode.get(replica);
        if (replicaId != null) {
            multiReplicaLsnCache.computeIfAbsent(
                replicaId,
                x -> new ThrottledCache<>(Clock.systemUTC(), LSN_CHECK_LOCK_TIMEOUT)
            )
                .get(() -> sequence.fetch(replica));
        }
    }

    private boolean isConsistent(Connection replica, long lastWrite) {
        final Long lsn = multiReplicaLsnCache.get(replicaNode.get(replica)).get().orElse(0L);
        return lsn >= lastWrite;
    }

}
