package com.atlassian.db.replica.it.example.aurora.replica.spi;

public interface Latency {
    void measure(ExceptionSpecificRunnable<Exception> runnable) throws Exception;
}
