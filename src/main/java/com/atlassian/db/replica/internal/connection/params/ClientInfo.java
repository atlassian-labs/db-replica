package com.atlassian.db.replica.internal.connection.params;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.util.Objects;
import java.util.Properties;

public final class ClientInfo {
    private final String name;
    private final String value;
    private final Properties properties;

    public ClientInfo(String value, String name) {
        this.value = value;
        this.name = name;
        this.properties = null;
    }

    public ClientInfo(Properties properties) {
        this.properties = properties;
        this.name = null;
        this.value = null;
    }

    public void configure(Connection connection) throws SQLClientInfoException {
        if (properties != null) {
            connection.setClientInfo(properties);
        } else {
            connection.setClientInfo(name, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientInfo that = (ClientInfo) o;
        return Objects.equals(name, that.name) && Objects.equals(value, that.value) && Objects.equals(
            properties,
            that.properties
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, properties);
    }

    @Override
    public String toString() {
        return "ClientInfo{" +
            "name='" + name + '\'' +
            ", value='" + value + '\'' +
            ", properties=" + properties +
            '}';
    }
}
