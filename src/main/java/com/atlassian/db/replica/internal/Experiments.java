package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.Experiment;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Experiments {
    private final Map<String, Experiment> experimentsMap;

    public Experiments(Collection<Experiment> experiments) {
        final Map<String, Experiment> experimentsByKey = new HashMap<>();
        for (Experiment experiment : experiments) {
            experimentsByKey.put(experiment.getKey(), experiment);
        }
        this.experimentsMap = experimentsByKey;
    }

    public boolean isEnabled(String key) {
        final Experiment experiment = experimentsMap.get(key);
        if (experiment == null) {
            return true;
        } else {
            return experiment.isEnabled();
        }
    }
}
