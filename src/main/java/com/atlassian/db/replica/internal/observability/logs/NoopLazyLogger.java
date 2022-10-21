package com.atlassian.db.replica.internal.observability.logs;

import java.util.function.Supplier;

public class NoopLazyLogger implements LazyLogger{
    @Override
    public void debug(Supplier<String> message) {
        //noop
    }

    @Override
    public void debug(Supplier<String> message, Throwable t) {
        //noop
    }

    @Override
    public void info(Supplier<String> message) {
        //noop
    }

    @Override
    public void info(Supplier<String> message, Throwable t) {
        //noop
    }

    @Override
    public void warn(Supplier<String> message) {
        //noop
    }

    @Override
    public void warn(Supplier<String> message, Throwable t) {
        //noop
    }

    @Override
    public void error(Supplier<String> message) {
        //noop
    }

    @Override
    public void error(Supplier<String> message, Throwable t) {
        //noop
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}
