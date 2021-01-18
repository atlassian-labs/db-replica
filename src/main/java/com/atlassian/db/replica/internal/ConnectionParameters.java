package com.atlassian.db.replica.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ConnectionParameters {

    private Boolean isAutoCommit;
    private Integer transactionIsolation;
    private String catalog;
    private Map<String, Class<?>> typeMap;
    private Integer holdability;

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
    }

    public void setTransactionIsolation(Integer transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    public Integer getTransactionIsolation() {
        return this.transactionIsolation;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.isAutoCommit = autoCommit;
    }

    public boolean isAutoCommit() {
        return isAutoCommit == null || isAutoCommit;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public Map<String, Class<?>> getTypeMap() {
        return typeMap == null ? Collections.emptyMap() : new HashMap<>(typeMap);
    }

    public void setTypeMap(Map<String, Class<?>> typeMap) {
        this.typeMap = typeMap;
    }

    public Integer getHoldability() {
        return holdability;
    }

    public void setHoldability(Integer holdability) {
        this.holdability = holdability;
    }

    @Override
    public String toString() {
        return "ConnectionParameters{" +
            "isAutoCommit=" + isAutoCommit +
            ", transactionIsolation=" + transactionIsolation +
            ", catalog='" + catalog + '\'' +
            ", typeMap=" + typeMap +
            ", holdability=" + holdability +
            '}';
    }
}
