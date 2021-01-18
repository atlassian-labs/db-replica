package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.internal.state.ConnectionState;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import com.atlassian.db.replica.spi.state.StateListener;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.Optional;

import static com.atlassian.db.replica.api.state.State.CLOSED;
import static com.atlassian.db.replica.api.state.State.MAIN;

public class ReplicaConnectionProvider implements AutoCloseable {
    private final ReplicaConsistency consistency;
    private final ConnectionState state;
    private final ConnectionParameters parameters;
    private Boolean isReadOnly;
    private final Warnings warnings;

    public ReplicaConnectionProvider(
        ConnectionProvider connectionProvider,
        ReplicaConsistency consistency,
        StateListener stateListener
    ) {
        this.parameters = new ConnectionParameters();
        this.warnings = new Warnings();
        this.state = new ConnectionState(connectionProvider, consistency, parameters, warnings, stateListener);
        this.consistency = consistency;
    }

    public Connection getWriteConnection() throws SQLException {
        return state.getWriteConnection();
    }

    public Connection getReadConnection() throws SQLException {
        return state.getReadConnection();
    }

    public void setTransactionIsolation(Integer transactionIsolation) {
        parameters.setTransactionIsolation(transactionIsolation);
    }

    public int getTransactionIsolation() throws SQLException {
        if (parameters.getTransactionIsolation() != null) {
            return parameters.getTransactionIsolation();
        } else {
            return state.getWriteConnection().getTransactionIsolation();
        }
    }

    public void setAutoCommit(Boolean autoCommit) throws SQLException {
        final boolean autoCommitBefore = getAutoCommit();
        parameters.setAutoCommit(autoCommit);
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().setAutoCommit(autoCommit);
        }
        if (autoCommitBefore != getAutoCommit()) {
            recordCommit(autoCommitBefore);
        }
    }

    public boolean getAutoCommit() {
        return parameters.isAutoCommit();
    }

    public Boolean isClosed() {
        return state.getState().equals(CLOSED);
    }

    public boolean getReadOnly() {
        return isReadOnly != null && isReadOnly;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        isReadOnly = readOnly;
        if (readOnly) {
            state.getReadConnection().setReadOnly(isReadOnly);
        } else {
            state.getWriteConnection().setReadOnly(isReadOnly);
        }
    }

    public String getCatalog() {
        return parameters.getCatalog();
    }

    public void setCatalog(String catalog) throws SQLException {
        parameters.setCatalog(catalog);
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().setCatalog(catalog);
        }
    }

    public Map<String, Class<?>> getTypeMap() {
        return parameters.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> typeMap) throws SQLException {
        parameters.setTypeMap(typeMap);
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().setTypeMap(typeMap);
        }
    }

    public Integer getHoldability() throws SQLException {
        return parameters.getHoldability() == null ? state.getWriteConnection().getHoldability() : parameters.getHoldability();
    }

    public void setHoldability(Integer holdability) throws SQLException {
        parameters.setHoldability(holdability);
        final Optional<Connection> connection = state.getConnection();
        if (connection.isPresent()) {
            connection.get().setHoldability(holdability);
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

    public <T> T unwrap(Class<T> iface) throws SQLException {
        final Connection currentConnection = state.getReadConnection();
        if (iface.isAssignableFrom(currentConnection.getClass())) {
            return iface.cast(currentConnection);
        } else {
            return currentConnection.unwrap(iface);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        final Connection currentConnection = state.getReadConnection();
        if (iface.isAssignableFrom(currentConnection.getClass())) {
            return true;
        } else {
            return currentConnection.isWrapperFor(iface);
        }
    }

    public boolean hasWriteConnection() {
        return state.getState().equals(MAIN);
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
            connection.get().commit();
            recordCommit(parameters.isAutoCommit());
        }
    }

    private void recordCommit(boolean autoCommit) throws SQLException {
        if (state.getState().equals(MAIN) && !autoCommit) {
            consistency.write(state.getWriteConnection());
        }
    }

    @Override
    public void close() throws SQLException {
        state.close();
    }
}
