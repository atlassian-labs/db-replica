package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.spi.DatabaseCluster;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

//TODO: promote to API?
public class MultiReplicaConsistency implements ReplicaConsistency {
    private final ReplicaConsistency replicaConsistency;
    private final DatabaseCluster cluster;

    public MultiReplicaConsistency(
        ReplicaConsistency replicaConsistency,
        DatabaseCluster cluster
    ) {
        this.replicaConsistency = replicaConsistency;
        this.cluster = cluster;
    }

    @Override
    public void write(Connection main) {
        replicaConsistency.write(main);
    }

    // TODO: currently it ignores the provided supplier and uses cluster. We could change the API to provide all
    //    // connections
    @Override
    public boolean isConsistent(Supplier<Connection> replicaSupplier) {
        try {
            return cluster.getReplicas().stream().allMatch(replica -> replicaConsistency.isConsistent(() -> {
                return replica.getConnectionSupplier().get();
            }));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);// TODO
        }
    }
}
