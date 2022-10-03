package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.internal.logs.LazyLogger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static java.lang.String.format;

public final class ConnectionParameters {
    private Boolean isAutoCommit;
    private Boolean readOnly;
    private Integer transactionIsolation;
    private String catalog;
    private Map<String, Class<?>> typeMap;
    private Integer holdability;
    private String schema;
    private ClientInfo clientInfo;
    private NetworkTimeout networkTimeout;
    private final List<String> runtimeParameterChanges = new CopyOnWriteArrayList<>();
    private final LazyLogger logger;

    public ConnectionParameters(LazyLogger logger) {
        this.logger = logger;
    }

    public void initialize(Connection connection) throws SQLException {
        logger.debug(() -> format("Initializing connection %s", connection));
        if (isAutoCommit != null) {
            logger.debug(() -> format("Initializing connection setAutoCommit(%s)", isAutoCommit));
            connection.setAutoCommit(isAutoCommit);
        }
        if (transactionIsolation != null) {
            logger.debug(() -> format("Initializing connection setTransactionIsolation(%s)", transactionIsolation));
            connection.setTransactionIsolation(transactionIsolation);
        }
        if (catalog != null) {
            logger.debug(() -> format("Initializing connection setCatalog(%s)", catalog));
            connection.setCatalog(catalog);
        }
        if (typeMap != null) {
            logger.debug(() -> format("Initializing connection setTypeMap(%s)", typeMap));
            connection.setTypeMap(typeMap);
        }
        if (holdability != null) {
            logger.debug(() -> format("Initializing connection setHoldability(%s)", holdability));
            connection.setHoldability(holdability);
        }
        if (readOnly != null) {
            logger.debug(() -> format("Initializing connection setReadOnly(%s)", readOnly));
            connection.setReadOnly(readOnly);
        }
        if (schema != null) {
            logger.debug(() -> format("Initializing connection setSchema(%s)", schema));
            connection.setSchema(schema);
        }
        if (clientInfo != null) {
            logger.debug(() -> format("Initializing connection configure clientInfo(%s)", clientInfo));
            clientInfo.configure(connection);
        }
        if (networkTimeout != null) {
            logger.debug(() -> format("Initializing connection configure networkTimeout(%s)", networkTimeout));
            networkTimeout.configure(connection);
        }
        initializeRuntimeParameters(connection);
    }

    private void initializeRuntimeParameters(Connection connection) throws SQLException {
        logger.debug(() -> format("Initializing runtimeParameters %s", connection));
        if (runtimeParameterChanges.isEmpty()) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            for (String parameterChange : runtimeParameterChanges) {
                logger.debug(() -> format("Initializing runtimeParameter %s", parameterChange));
                statement.execute(parameterChange);
            }
        }
    }

    public void addRuntimeParameterConfiguration(
        String parameterConfiguration
    ) {
        runtimeParameterChanges.add(parameterConfiguration);
    }

    public void setTransactionIsolation(
        Supplier<Optional<Connection>> currentConnection,
        Integer transactionIsolation
    ) throws SQLException {
        executeIfPresent(currentConnection, connection -> {
            logger.debug(() -> format("connection(%s)#setTransactionIsolation(%s)", connection, transactionIsolation));
            connection.setTransactionIsolation(transactionIsolation);
        });
        this.transactionIsolation = transactionIsolation;
    }

    public Integer getTransactionIsolation() {
        return this.transactionIsolation;
    }

    public void setAutoCommit(
        Supplier<Optional<Connection>> currentConnection,
        Boolean autoCommit
    ) throws SQLException {
        executeIfPresent(currentConnection, connection -> {
            logger.debug(() -> format("connection(%s)#setAutoCommit(%s)", connection, autoCommit));
            connection.setAutoCommit(autoCommit);
        });
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
        executeIfPresent(currentConnection, connection -> {
            logger.debug(() -> format("connection(%s)#setReadOnly(%s)", connection, readOnly));
            connection.setReadOnly(readOnly);
        });
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
        executeIfPresent(currentConnection, connection -> connection.setSchema(schema));
        this.schema = schema;
    }

    public void setClientInfo(
        Supplier<Optional<Connection>> currentConnection,
        ClientInfo clientInfo
    ) throws SQLException {
        executeIfPresent(currentConnection, clientInfo::configure);
        this.clientInfo = clientInfo;
    }

    public void setNetworkTimeout(
        Supplier<Optional<Connection>> currentConnection,
        NetworkTimeout networkTimeout
    ) throws SQLException {
        executeIfPresent(currentConnection, connection -> {
            logger.debug(() -> format("connection(%s)#setNetworkTimeout(%s)", connection, networkTimeout));
            networkTimeout.configure(connection);
        });
        this.networkTimeout = networkTimeout;
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
            ", clientInfo=" + clientInfo +
            ", networkTimeout=" + networkTimeout +
            ", runtimeParameterChanges=" + runtimeParameterChanges +
            '}';
    }
}
