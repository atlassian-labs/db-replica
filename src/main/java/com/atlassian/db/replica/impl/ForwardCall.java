package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.spi.DualCall;

import java.sql.SQLException;

public class ForwardCall implements DualCall {
    @Override
    public <T> T callReplica(SqlCall<T> call) throws SQLException {
        return call.call();
    }

    @Override
    public <T> T callMain(SqlCall<T> call) throws SQLException {
        return call.call();
    }
}
