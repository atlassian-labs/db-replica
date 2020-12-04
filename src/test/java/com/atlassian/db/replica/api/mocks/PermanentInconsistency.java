package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.*;

import java.sql.*;
import java.util.function.Supplier;

public class PermanentInconsistency implements ReplicaConsistency {
    private final boolean shouldUseReplicaSupplier;

    public PermanentInconsistency(boolean shouldUseReplicaSupplier) {
        this.shouldUseReplicaSupplier = shouldUseReplicaSupplier;
    }

    public PermanentInconsistency() {
        this.shouldUseReplicaSupplier = true;
    }

    @Override
    public void write(Connection mainConnection) {

    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        if(this.shouldUseReplicaSupplier){
            replica.get();
        }
        return false;
    }
}
