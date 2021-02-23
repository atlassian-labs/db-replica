package com.atlassian.db.replica.it.consistency;

import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.util.function.Supplier;

public class WaitingReplicaConsistency implements ReplicaConsistency {
    private final ReplicaConsistency consistency;

    public WaitingReplicaConsistency(ReplicaConsistency consistency) {
        this.consistency = consistency;
    }

    @Override
    public void write(Connection main) {
        consistency.write(main);
    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        for (int i = 0; i < 30; i++) {
            if (consistency.isConsistent(replica)) {
                return true;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Replica is still inconsistent after 30s.");
    }
}
