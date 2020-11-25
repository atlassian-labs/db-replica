package com.atlassian.db.replica.internal.util;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestComparables {

    @Test
    public void shouldReturnMaxValue() {
        final Integer max = Comparables.max(1, 2);

        assertThat(max).isEqualTo(2);
    }

    @Test
    public void shouldReturnEqualValue() {
        final Integer max = Comparables.max(5, 5);

        assertThat(max).isEqualTo(5);
    }

}