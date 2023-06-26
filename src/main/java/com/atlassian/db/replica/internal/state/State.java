package com.atlassian.db.replica.internal.state;


import java.util.Objects;

public final class State {
    private final String name;

    private State(String name) {
        this.name = name;
    }

    public static final State NOT_INITIALISED = new State("NOT_INITIALISED");
    public static final State MAIN = new State("MAIN");
    public static final State REPLICA = new State("REPLICA");
    public static final State CLOSED = new State("CLOSED");
    public static final State COMMITED_MAIN = new State("COMMITED_MAIN");

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "State{" +
            "name='" + name + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        State states = (State) o;
        return Objects.equals(name, states.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
