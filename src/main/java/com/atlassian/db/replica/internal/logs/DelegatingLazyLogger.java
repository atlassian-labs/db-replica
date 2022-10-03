package com.atlassian.db.replica.internal.logs;

import com.atlassian.db.replica.spi.Logger;

import java.util.function.Supplier;

public class DelegatingLazyLogger implements LazyLogger {
    private final Logger log;

    public DelegatingLazyLogger(Logger log) {
        this.log = log;
    }

    public void debug(Supplier<String> message) {
        log.debug(message.get());
    }

    public void debug(Supplier<String> message, Throwable t) {
        log.debug(message.get(), t);
    }

    public void info(Supplier<String> message) {
        log.info(message.get());
    }

    public void info(Supplier<String> message, Throwable t) {
        log.info(message.get(), t);
    }

    public void warn(Supplier<String> message) {
        log.warn(message.get());
    }

    public void warn(Supplier<String> message, Throwable t) {
        log.warn(message.get(), t);
    }

    public void error(Supplier<String> message) {
        log.error(message.get());
    }

    public void error(Supplier<String> message, Throwable t) {
        log.error(message.get(), t);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
