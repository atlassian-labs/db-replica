package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.Experiment;

public final class StaticExperiment implements Experiment {
    private final boolean enabled;
    private final String key;


    public StaticExperiment(String key, boolean enabled) {
        this.key = key;
        this.enabled = enabled;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
}
