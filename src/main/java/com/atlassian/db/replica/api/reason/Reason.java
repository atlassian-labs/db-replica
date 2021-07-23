package com.atlassian.db.replica.api.reason;

import java.util.Objects;

/**
 * Describes the reason to choose either replica or main database connection.
 */
public final class Reason {
    private final String name;
    private final boolean isRunOnMain;
    private final boolean isWrite;

    private Reason(final String name, boolean isRunOnMain, boolean isWrite) {
        this.name = name;
        this.isRunOnMain = isRunOnMain;
        this.isWrite = isWrite;
    }

    public static final Reason RW_API_CALL =
            new ReasonBuilder("RW_API_CALL").isRunOnMain(true).isWrite(true).build();
    public static final Reason REPLICA_INCONSISTENT =
            new ReasonBuilder("REPLICA_INCONSISTENT").isRunOnMain(true).isWrite(false).build();
    public static final Reason READ_OPERATION =
            new ReasonBuilder("READ_OPERATION").isRunOnMain(false).isWrite(false).build();
    public static final Reason WRITE_OPERATION =
            new ReasonBuilder("WRITE_OPERATION").isRunOnMain(true).isWrite(true).build();
    public static final Reason LOCK =
            new ReasonBuilder("LOCK").isRunOnMain(true).isWrite(false).build();
    public static final Reason MAIN_CONNECTION_REUSE =
            new ReasonBuilder("MAIN_CONNECTION_REUSE").isRunOnMain(true).isWrite(false).build();
    public static final Reason HIGH_TRANSACTION_ISOLATION_LEVEL =
            new ReasonBuilder("HIGH_TRANSACTION_ISOLATION_LEVEL").isRunOnMain(true).isWrite(false).build();
    public static final Reason RO_API_CALL =
            new ReasonBuilder("RO_API_CALL").isRunOnMain(false).isWrite(false).build();
    public static final Reason REPLICA_GET_FAILURE =
        new ReasonBuilder("REPLICA_GET_FAILURE").isRunOnMain(true).isWrite(false).build();

    public String getName() {
        return name;
    }

    boolean isRunOnMain() {
        return isRunOnMain;
    }

    boolean isWrite() {
        return isWrite;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reason reason = (Reason) o;
        return isRunOnMain == reason.isRunOnMain && isWrite == reason.isWrite && Objects.equals(name, reason.name);
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
            ", isWrite=" + isWrite +
            '}';
    }

    private static class ReasonBuilder {
        private final String name;
        private boolean isRunOnMain = false;
        private boolean isWrite = false;

        ReasonBuilder(final String name) {
            this.name = name;
        }

        ReasonBuilder isRunOnMain(boolean isRunOnMain) {
            this.isRunOnMain = isRunOnMain;
            return this;
        }

        ReasonBuilder isWrite(boolean isWrite) {
            this.isWrite = isWrite;
            return this;
        }

        Reason build() {
            return new Reason(name, isRunOnMain, isWrite);
        }
    }
}
