package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.internal.state.ConnectionState;

import java.sql.SQLException;


public class ReplicaConnectionProvider {
    private final ConnectionState state;
    private final ConnectionParameters parameters;

    public ReplicaConnectionProvider(
        ConnectionParameters parameters, ConnectionState state
    ) {
        this.parameters = parameters;
        this.state = state;
    }

    public void setTransactionIsolation(Integer transactionIsolation) throws SQLException {
        parameters.setTransactionIsolation(state::getConnection, transactionIsolation);
    }

    public int getTransactionIsolation() throws SQLException {
        if (parameters.getTransactionIsolation() != null) {
            return parameters.getTransactionIsolation();
        } else {
            return state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).getTransactionIsolation();
        }
    }

}
