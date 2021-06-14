package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.PessimisticPropagationConsistency;
import com.atlassian.db.replica.internal.util.ConnectionSupplier;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;

import java.sql.Connection;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class PessimisticPropagationConsistencyTest {

    private PessimisticPropagationConsistency.Builder consistencyBuilder;
    private MutableClock clock;
    private Connection main;
    private Connection replica;

    @BeforeEach
    public void resetState() {
        clock = MutableClock.epochUTC();
        consistencyBuilder = new PessimisticPropagationConsistency.Builder()
            .measureTime(clock)
            .cacheLastWrite(new VolatileCache<>());
        main = null;
        replica = null;
    }

    @Test
    public void shouldBeConsistentAfterPropagation() {
        ReplicaConsistency consistency = consistencyBuilder
            .assumeMaxPropagation(Duration.ofMillis(200))
            .build();

        consistency.write(main);
        clock.add(Duration.ofMillis(300));
        boolean consistent = consistency.isConsistent(new ConnectionSupplier(replica));

        assertThat(consistent).isTrue();
    }

    @Test
    public void shouldBeInconsistentBeforePropagation() {
        ReplicaConsistency consistency = consistencyBuilder
            .assumeMaxPropagation(Duration.ofMillis(200))
            .build();

        consistency.write(main);
        clock.add(Duration.ofMillis(50));
        boolean consistent = consistency.isConsistent(new ConnectionSupplier(replica));

        assertThat(consistent).isFalse();
    }

    @Test
    public void shouldAssumeInconsistencyWhenUnknown() {
        ReplicaConsistency consistency = consistencyBuilder
            .assumeMaxPropagation(Duration.ofMillis(200))
            .build();

        clock.add(Duration.ofMillis(700));
        boolean consistent = consistency.isConsistent(new ConnectionSupplier(replica));

        assertThat(consistent).isFalse();
    }

    @Test
    public void shouldNotAssumeInconsistentForeverWhenUnknown() {
        ReplicaConsistency consistency = consistencyBuilder
            .assumeMaxPropagation(Duration.ofMillis(200))
            .build();

        clock.add(Duration.ofMillis(700));
        boolean isConsistent = consistency.isConsistent(new ConnectionSupplier(replica));
        clock.add(Duration.ofMillis(700));
        boolean isConsistentLater = consistency.isConsistent(new ConnectionSupplier(replica));

        assertThat(isConsistent).isFalse();
        assertThat(isConsistentLater).isTrue();
    }
}
