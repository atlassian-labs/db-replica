package com.atlassian.db.replica.spi;

public interface Experiment {
    String getKey();

    boolean isEnabled();
}
