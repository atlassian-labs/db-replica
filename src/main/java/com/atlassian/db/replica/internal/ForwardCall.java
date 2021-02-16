package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.spi.DatabaseCall;

import java.sql.SQLException;

public class ForwardCall implements DatabaseCall {

    @Override
    public <T> T call(final SqlCall<T> call, RouteDecision decision) throws SQLException {
        return call.call();
    }

}
