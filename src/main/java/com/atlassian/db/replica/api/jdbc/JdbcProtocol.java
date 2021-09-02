package com.atlassian.db.replica.api.jdbc;

import java.util.Objects;

public final class JdbcProtocol {
    public static final JdbcProtocol POSTGRES = new JdbcProtocol("postgresql");

    private final String protocol;

    private JdbcProtocol(String protocol) {
        this.protocol = protocol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcProtocol that = (JdbcProtocol) o;
        return Objects.equals(protocol, that.protocol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocol);
    }

    @Override
    public String toString() {
        return protocol;
    }
}
