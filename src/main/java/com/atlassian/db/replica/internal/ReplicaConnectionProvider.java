package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.internal.state.ConnectionState;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Optional;


public class ReplicaConnectionProvider {
    private final ConnectionState state;
    private final ConnectionParameters parameters;
    private final Warnings warnings;

    public ReplicaConnectionProvider(
        ConnectionParameters parameters, Warnings warnings, ConnectionState state
    ) {
        this.parameters = parameters;
        this.warnings = warnings;
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

    public SQLWarning getWarning() throws SQLException {
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            warnings.saveWarning(connection.get().getWarnings());
        }
        return warnings.getWarning();
    }

    public void clearWarnings() throws SQLException {
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().clearWarnings();
        }
        warnings.clear();
    }
}
