package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.ReadReplicaUnsupportedOperationException;
import com.atlassian.db.replica.spi.Cache;
import com.atlassian.db.replica.spi.CircuitBreaker;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Optional;

/**
 * Trips when coverage for {@link java.sql} is missing.
 * Never resets.
 */
public class MissingCoverageBreaker implements CircuitBreaker {

    private final Cache<Boolean> coverageSufficient;

    public static Cache<Boolean> cacheStatically() {
        return new StaticFlag();
    }

    /**
     * @param coverageSufficient remembers if coverage is still sufficient
     */
    public MissingCoverageBreaker(Cache<Boolean> coverageSufficient) {
        this.coverageSufficient = coverageSufficient;
    }

    @Override
    public boolean canCreateDualConnection() {
        return coverageSufficient.get().orElse(true);
    }

    @Override
    public <T> T handle(SqlCall<T> call) throws SQLException {
        try {
            return call.call();
        } catch (ReadReplicaUnsupportedOperationException | SQLFeatureNotSupportedException e) {
            throw handleMissingCoverage(e);
        }
    }

    @Override
    public void handle(SqlRun run) throws SQLException {
        try {
            run.run();
        } catch (ReadReplicaUnsupportedOperationException | SQLFeatureNotSupportedException e) {
            throw handleMissingCoverage(e);
        }
    }

    private RuntimeException handleMissingCoverage(Exception e) {
        coverageSufficient.put(false);
        String ticket = "https://github.com/atlassian-labs/db-replica/issues/new";
        return new RuntimeException("a method needs to be covered, sanitize and post the stacktrace to " + ticket, e);
    }


    /**
     * Holds a flag state across instances in the same class loader.
     */
    private static class StaticFlag implements Cache<Boolean> {

        private static volatile Boolean FLAG = null;

        @Override
        public Optional<Boolean> get() {
            return Optional.ofNullable(FLAG);
        }

        @Override
        public void put(Boolean value) {
            FLAG = value;
        }

        @Override
        public void reset() {
            FLAG = null;
        }
    }

}
