package com.atlassian.db.replica.it.example.aurora.replica.spi;

/**
 * An interface that is similar to a {@link Runnable} except that the {@link #run()} method can throw a specific checked
 * exception.
 * @param <E> the type of the exception that may be thrown
 */
public interface ExceptionSpecificRunnable<E extends Exception> {

    void run() throws E;
}

