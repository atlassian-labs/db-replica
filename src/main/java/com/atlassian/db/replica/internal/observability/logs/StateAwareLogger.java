package com.atlassian.db.replica.internal.observability.logs;

import com.atlassian.db.replica.internal.connection.state.State;

import java.util.function.Supplier;

public class StateAwareLogger implements LazyLogger {
    private final Supplier<State> stateSupplier;
    private final LazyLogger logger;

    public StateAwareLogger(Supplier<State> stateSupplier, LazyLogger logger) {
        this.stateSupplier = stateSupplier;
        this.logger = logger;
    }

    @Override
    public void debug(Supplier<String> message) {
        if (isEnabled()) {
            logger.debug(messageWithStatus(message));
        }
    }

    @Override
    public void debug(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.debug(messageWithStatus(message), t);
        }
    }

    @Override
    public void info(Supplier<String> message) {
        if (isEnabled()) {
            logger.info(messageWithStatus(message));
        }
    }

    @Override
    public void info(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.info(messageWithStatus(message), t);
        }
    }

    @Override
    public void warn(Supplier<String> message) {
        if (isEnabled()) {
            logger.warn(messageWithStatus(message));
        }
    }

    @Override
    public void warn(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.warn(messageWithStatus(message), t);
        }
    }

    @Override
    public void error(Supplier<String> message) {
        if (isEnabled()) {
            logger.error(messageWithStatus(message));
        }
    }

    @Override
    public void error(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.error(messageWithStatus(message), t);
        }
    }

    @Override
    public boolean isEnabled() {
        return logger.isEnabled();
    }

    private Supplier<String> messageWithStatus(Supplier<String> message) {
        return () -> "[state=" + stateSupplier.get().getName() + "] " + message.get();
    }
}
