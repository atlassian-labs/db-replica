package com.atlassian.db.replica.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.Executor;

public final class NetworkTimeout {
    private final Executor executor;
    private final int milliseconds;

    public NetworkTimeout(Executor executor, int milliseconds) {
        this.executor = executor;
        this.milliseconds = milliseconds;
    }

    public void configure(Connection connection) throws SQLException {
        connection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NetworkTimeout that = (NetworkTimeout) o;
        return milliseconds == that.milliseconds && Objects.equals(executor, that.executor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executor, milliseconds);
    }

    @Override
    public String toString() {
        return "NetworkTimeout{" +
            "executor=" + executor +
            ", milliseconds=" + milliseconds +
            '}';
    }
}
