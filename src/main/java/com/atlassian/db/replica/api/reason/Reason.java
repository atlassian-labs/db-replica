package com.atlassian.db.replica.api.reason;

import java.util.Objects;

/**
 * Describes the reason to choose either replica or main database connection.
 */
public final class Reason {
    private final String name;
    private final boolean isRunOnMain;

    private Reason(final String name, boolean isRunOnMain) {
        this.name = name;
        this.isRunOnMain = isRunOnMain;
    }

    public static final Reason RW_API_CALL = new Reason("RW_API_CALL", true);
    public static final Reason REPLICA_INCONSISTENT = new Reason("REPLICA_INCONSISTENT", true);
    public static final Reason READ_OPERATION = new Reason("READ_OPERATION", false);
    public static final Reason WRITE_OPERATION = new Reason("WRITE_OPERATION", true);
    public static final Reason LOCK = new Reason("LOCK", true);
    public static final Reason MAIN_CONNECTION_REUSE = new Reason(
        "MAIN_CONNECTION_REUSE",
        true
    );
    public static final Reason HIGH_TRANSACTION_ISOLATION_LEVEL = new Reason(
        "HIGH_TRANSACTION_ISOLATION_LEVEL",
        true
    );
    public static final Reason RO_API_CALL = new Reason("RO_API_CALL", false);

    public String getName() {
        return name;
    }

    public boolean isRunOnMain() {
        return isRunOnMain;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reason reason = (Reason) o;
        return isRunOnMain == reason.isRunOnMain && Objects.equals(name, reason.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isRunOnMain);
    }

    @Override
    public String toString() {
        return "Reason{" +
            "name='" + name + '\'' +
            ", isRunOnMain=" + isRunOnMain +
            '}';
    }
}
