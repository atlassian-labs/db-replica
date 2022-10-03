package com.atlassian.db.replica.internal.logs;

import java.util.function.Supplier;

public class TaggedLogger implements LazyLogger {
    private final String key;
    private final String value;
    private final LazyLogger logger;

    public TaggedLogger(String key, String value, LazyLogger logger) {
        this.key = key;
        this.value = value;
        this.logger = logger;
    }

    @Override
    public void debug(Supplier<String> message) {
        if (isEnabled()) {
            logger.debug(messageWithDualConnectionUuid(message));
        }
    }

    @Override
    public void debug(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.debug(messageWithDualConnectionUuid(message), t);
        }
    }

    @Override
    public void info(Supplier<String> message) {
        if (isEnabled()) {
            logger.info(messageWithDualConnectionUuid(message));
        }
    }

    @Override
    public void info(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.info(messageWithDualConnectionUuid(message), t);
        }
    }

    @Override
    public void warn(Supplier<String> message) {
        if (isEnabled()) {
            logger.warn(messageWithDualConnectionUuid(message));
        }
    }

    @Override
    public void warn(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.warn(messageWithDualConnectionUuid(message), t);
        }
    }

    @Override
    public void error(Supplier<String> message) {
        if (isEnabled()) {
            logger.error(messageWithDualConnectionUuid(message));
        }
    }

    @Override
    public void error(Supplier<String> message, Throwable t) {
        if (isEnabled()) {
            logger.error(messageWithDualConnectionUuid(message), t);
        }
    }

    @Override
    public boolean isEnabled() {
        return logger.isEnabled();
    }

    private Supplier<String> messageWithDualConnectionUuid(Supplier<String> message) {
        return () -> "[" + key + "=" + value + "] " + message.get();
    }
}
