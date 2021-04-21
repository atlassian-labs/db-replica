package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.internal.state.ConnectionState;
import com.atlassian.db.replica.internal.state.State;
import com.atlassian.db.replica.internal.state.StateListener;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.atlassian.db.replica.api.reason.Reason.RO_API_CALL;
import static com.atlassian.db.replica.internal.state.State.CLOSED;
import static com.atlassian.db.replica.internal.state.State.MAIN;

public class ReplicaConnectionProvider implements AutoCloseable {
    private final ReplicaConsistency consistency;
    private final ConnectionState state;
    private final ConnectionParameters parameters;
    private final Warnings warnings;
    private final boolean compatibleWithPreviousVersion;

    public ReplicaConnectionProvider(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency,
        StateListener stateListener,
        boolean compatibleWithPreviousVersion
    ) {
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
        this.parameters = new ConnectionParameters();
        this.warnings = new Warnings();
        this.state = new ConnectionState(connectionProvider, consistency, parameters, warnings, stateListener);
        this.consistency = consistency;
    }

    public Connection getWriteConnection(RouteDecisionBuilder decisionBuilder) throws SQLException {
        return state.getWriteConnection(decisionBuilder);
    }

    public Connection getReadConnection(RouteDecisionBuilder decisionBuilder) throws SQLException {
        return state.getReadConnection(decisionBuilder);
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

    public void setAutoCommit(Boolean autoCommit) throws SQLException {
        final boolean autoCommitBefore = getAutoCommit();
        if (!compatibleWithPreviousVersion) {
            if (autoCommitBefore != autoCommit) {
                preCommit(autoCommitBefore);
            }
        }
        parameters.setAutoCommit(state::getConnection, autoCommit);
        if (autoCommitBefore != getAutoCommit()) {
            recordCommit(autoCommitBefore);
        }
    }

    public void setSchema(String schema) throws SQLException {
        parameters.setSchema(state::getConnection, schema);
    }

    public void setClientInfo(ClientInfo clientInfo) throws SQLException {
        parameters.setClientInfo(state::getConnection, clientInfo);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        parameters.setNetworkTimeout(state::getConnection, new NetworkTimeout(executor, milliseconds));
    }

    public boolean getAutoCommit() {
        return parameters.isAutoCommit();
    }

    public Boolean isClosed() {
        return state.getState().equals(CLOSED);
    }

    public boolean getReadOnly() {
        return parameters.isReadOnly();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        parameters.setReadOnly(state::getConnection, readOnly);
    }

    public String getCatalog() {
        return parameters.getCatalog();
    }

    public void setCatalog(String catalog) throws SQLException {
        parameters.setCatalog(state::getConnection, catalog);
    }

    public Map<String, Class<?>> getTypeMap() {
        return parameters.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> typeMap) throws SQLException {
        parameters.setTypeMap(state::getConnection, typeMap);
    }

    public Integer getHoldability() throws SQLException {
        return parameters.getHoldability() == null ? state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL)).getHoldability() : parameters.getHoldability();
    }

    public void setHoldability(Integer holdability) throws SQLException {
        parameters.setHoldability(state::getConnection, holdability);
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

    public <T> T unwrap(Class<T> iface) throws SQLException {
        final Connection currentConnection = state.getReadConnection(new RouteDecisionBuilder(RO_API_CALL));
        if (iface.isAssignableFrom(currentConnection.getClass())) {
            return iface.cast(currentConnection);
        } else {
            return currentConnection.unwrap(iface);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        final Connection currentConnection = state.getReadConnection(new RouteDecisionBuilder(RO_API_CALL));
        if (iface.isAssignableFrom(currentConnection.getClass())) {
            return true;
        } else {
            return currentConnection.isWrapperFor(iface);
        }
    }

    public State getState() {
        return this.state.getState();
    }

    public Optional<RouteDecision> getStateDecision() {
        return this.state.getDecision();
    }

    public void rollback() throws SQLException {
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().rollback();
        }
    }

    public void commit() throws SQLException {
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            if (!compatibleWithPreviousVersion) {
                preCommit(parameters.isAutoCommit());
            }
            connection.get().commit();
            recordCommit(parameters.isAutoCommit());
        }
    }

    private void recordCommit(boolean autoCommit) throws SQLException {
        if (state.getState().equals(MAIN) && !autoCommit) {
            final Connection mainConnection = state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL));
            consistency.write(mainConnection);
        }
    }

    private void preCommit(boolean autoCommit) throws SQLException {
        if (state.getState().equals(MAIN) && !autoCommit) {
            final Connection mainConnection = state.getWriteConnection(new RouteDecisionBuilder(Reason.RW_API_CALL));
            consistency.preCommit(mainConnection);
        }
    }

    @Override
    public void close() throws SQLException {
        state.close();
    }

    public void abort(Executor executor) throws SQLException {
        state.abort(executor);
    }
}
