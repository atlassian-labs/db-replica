package com.atlassian.db.replica.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public final class ConnectionParameters {
    private final boolean compatibleWithPreviousVersion;
    private Boolean isAutoCommit;
    private Boolean readOnly;
    private Integer transactionIsolation;
    private String catalog;
    private Map<String, Class<?>> typeMap;
    private Integer holdability;
    private String schema;
    private ClientInfo clientInfo;

    public ConnectionParameters(boolean compatibleWithPreviousVersion) {
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
    }

    public void initialize(Connection connection) throws SQLException {
        if (isAutoCommit != null) {
            connection.setAutoCommit(isAutoCommit);
        }
        if (transactionIsolation != null) {
            connection.setTransactionIsolation(transactionIsolation);
        }
        if (catalog != null) {
            connection.setCatalog(catalog);
        }
        if (typeMap != null) {
            connection.setTypeMap(typeMap);
        }
        if (holdability != null) {
            connection.setHoldability(holdability);
        }
        if (readOnly != null) {
            connection.setReadOnly(readOnly);
        }
        if (!compatibleWithPreviousVersion) {
            if (schema != null) {
                connection.setSchema(schema);
            }
            if (clientInfo != null) {
                clientInfo.configure(connection);
            }
        }
    }

    public void setTransactionIsolation(
        Supplier<Optional<Connection>> currentConnection,
        Integer transactionIsolation
    ) throws SQLException {
        executeIfPresent(currentConnection, connection -> connection.setTransactionIsolation(transactionIsolation));
        this.transactionIsolation = transactionIsolation;
    }

    public Integer getTransactionIsolation() {
        return this.transactionIsolation;
    }

    public void setAutoCommit(
        Supplier<Optional<Connection>> currentConnection,
        Boolean autoCommit
    ) throws SQLException {
        executeIfPresent(currentConnection, connection -> connection.setAutoCommit(autoCommit));
        this.isAutoCommit = autoCommit;
    }

    public boolean isAutoCommit() {
        return isAutoCommit == null || isAutoCommit;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(Supplier<Optional<Connection>> currentConnection, String catalog) throws SQLException {
        executeIfPresent(currentConnection, connection -> connection.setCatalog(catalog));
        this.catalog = catalog;
    }

    public Map<String, Class<?>> getTypeMap() {
        return typeMap == null ? Collections.emptyMap() : new HashMap<>(typeMap);
    }

    public void setTypeMap(
        Supplier<Optional<Connection>> currentConnection,
        Map<String, Class<?>> typeMap
    ) throws SQLException {
        executeIfPresent(currentConnection, connection -> connection.setTypeMap(typeMap));
        this.typeMap = typeMap;
    }

    public Integer getHoldability() {
        return holdability;
    }

    public void setHoldability(
        Supplier<Optional<Connection>> currentConnection,
        Integer holdability
    ) throws SQLException {
        executeIfPresent(currentConnection, connection -> connection.setHoldability(holdability));
        this.holdability = holdability;
    }

    public boolean isReadOnly() {
        return readOnly != null && readOnly;
    }

    public void setReadOnly(Supplier<Optional<Connection>> currentConnection, boolean readOnly) throws SQLException {
        executeIfPresent(currentConnection, connection -> connection.setReadOnly(readOnly));
        this.readOnly = readOnly;
    }

    private void executeIfPresent(
        Supplier<Optional<Connection>> currentConnection,
        ConnectionOperation operation
    ) throws SQLException {
        final Optional<Connection> connection = currentConnection.get();
        if (connection.isPresent()) {
            operation.accept(connection.get());
        }
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(Supplier<Optional<Connection>> currentConnection, String schema) throws SQLException {
        if (!compatibleWithPreviousVersion) {
            executeIfPresent(currentConnection, connection -> connection.setSchema(schema));
            this.schema = schema;
        }
    }

    public void setClientInfo(Supplier<Optional<Connection>> currentConnection, ClientInfo clientInfo) throws SQLException {
        if (!compatibleWithPreviousVersion) {
            executeIfPresent(currentConnection, clientInfo::configure);
            this.clientInfo = clientInfo;
        }
    }

    @Override
    public String toString() {
        return "ConnectionParameters{" +
            "isAutoCommit=" + isAutoCommit +
            ", readOnly=" + readOnly +
            ", transactionIsolation=" + transactionIsolation +
            ", catalog='" + catalog + '\'' +
            ", typeMap=" + typeMap +
            ", holdability=" + holdability +
            ", schema='" + schema + '\'' +
            '}';
    }
}
