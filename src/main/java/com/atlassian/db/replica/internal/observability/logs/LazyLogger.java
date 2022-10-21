package com.atlassian.db.replica.internal.observability.logs;

import java.util.function.Supplier;

public interface LazyLogger {
    void debug(Supplier<String> message);

    void debug(Supplier<String> message, Throwable t);

    void info(Supplier<String> message);

    void info(Supplier<String> message, Throwable t);

    void warn(Supplier<String> message);

    void warn(Supplier<String> message, Throwable t);

    void error(Supplier<String> message);

    void error(Supplier<String> message, Throwable t);

    boolean isEnabled();
}
