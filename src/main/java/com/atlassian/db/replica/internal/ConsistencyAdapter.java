package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.Database;
import com.atlassian.db.replica.spi.ClusterConsistency;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;

public class ConsistencyAdapter implements ClusterConsistency {
    private final ReplicaConsistency replicaConsistency;

    public ConsistencyAdapter(ReplicaConsistency replicaConsistency) {
        this.replicaConsistency = replicaConsistency;
    }

    @Override
    public void write(Connection main) {
        replicaConsistency.write(main);
    }

    @Override
    public void preCommit(Connection main) {
        replicaConsistency.preCommit(main);
    }

    @Override
    public Optional<Database> getConsistent(Collection<Database> replicas) throws SQLException {
        if (replicas.size() != 1) {
            throw new RuntimeException("TODO");
        }
        return replicas
            .stream()
            .findFirst()
            .filter(replica -> replicaConsistency.isConsistent(replica.getConnectionSupplier()));
    }
}
