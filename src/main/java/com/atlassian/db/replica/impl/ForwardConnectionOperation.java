package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.api.SqlOperation;
import com.atlassian.db.replica.spi.DualConnectionOperation;

import java.sql.SQLException;

public class ForwardConnectionOperation implements DualConnectionOperation {
    @Override
    public <T> T executeOnReplica(SqlOperation<T> operation) throws SQLException {
        return operation.execute();
    }

    @Override
    public <T> T executeOnMain(SqlOperation<T> operation) throws SQLException {
        return operation.execute();
    }
}
