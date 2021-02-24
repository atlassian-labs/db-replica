package com.atlassian.db.replica.internal;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Collections;

import static com.google.common.collect.ImmutableList.of;

public class ExperimentsTest {

    @Test
    public void shouldBeEnabledByDefault() {
        final Experiments experiments = new Experiments(Collections.emptyList());

        final boolean featureEnabled = experiments.isEnabled("test");

        Assertions.assertThat(featureEnabled).isTrue();
    }

    @Test
    public void shouldBeEnabled() {
        final Experiments experiments = new Experiments(
            of(new StaticExperiment("test", true), new StaticExperiment("test2", false))
        );

        final boolean featureEnabled = experiments.isEnabled("test");

        Assertions.assertThat(featureEnabled).isTrue();
    }

    @Test
    public void shouldBeDisabled() {
        final Experiments experiments = new Experiments(
            of(new StaticExperiment("test", true), new StaticExperiment("test2", false))
        );

        final boolean featureEnabled = experiments.isEnabled("test2");

        Assertions.assertThat(featureEnabled).isFalse();
    }


}
