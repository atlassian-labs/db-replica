package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.*;
import com.atlassian.db.replica.spi.*;

import java.sql.*;

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
