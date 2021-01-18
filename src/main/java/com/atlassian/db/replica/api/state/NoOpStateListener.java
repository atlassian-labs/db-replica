package com.atlassian.db.replica.api.state;

import com.atlassian.db.replica.spi.state.StateListener;

public final class NoOpStateListener implements StateListener {
    @Override
    public void transition(State from, State to) {
        // NoOp implementation.
    }
}
