package com.atlassian.db.replica.spi.state;

import com.atlassian.db.replica.api.state.State;


public interface StateListener {

    /**
     * Informs that {@link com.atlassian.db.replica.api.DualConnection } changed {@link State}.
     *
     * @param from {@link State} before the transition.
     * @param to   {@link State} after the transition.
     */
    void transition(State from, State to);
}
