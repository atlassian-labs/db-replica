package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.*;

import java.sql.*;
import java.util.function.Supplier;

public class PermanentConsistency implements ReplicaConsistency {

    @Override
    public void write(Connection mainConnection) {

    }

    @Override
    public boolean isConsistent(Supplier<Connection> replica) {
        return true;
    }
}
