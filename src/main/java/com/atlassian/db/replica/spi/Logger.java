package com.atlassian.db.replica.spi;

public interface Logger {

    default void debug(String message) {}

    default void debug(String message, Throwable t) {}

    default void info(String message) {}

    default void info(String message, Throwable t) {}

    default void warn(String message) {}

    default void warn(String message, Throwable t) {}

    default void error(String message) {}

    default void error(String message, Throwable t) {}

}
