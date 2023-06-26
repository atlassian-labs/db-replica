package com.atlassian.db.replica.internal.state;

public final class NoOpStateListener implements StateListener {
    @Override
    public void transition(State from, State to) {
        // NoOp implementation.
    }
}
