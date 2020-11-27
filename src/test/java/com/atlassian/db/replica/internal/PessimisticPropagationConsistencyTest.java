package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.*;
import org.junit.*;
import org.threeten.extra.*;

import java.sql.*;
import java.time.*;

import static org.assertj.core.api.Assertions.*;

public class PessimisticPropagationConsistencyTest {

    private MutableClock clock;
    private Cache<Instant> lastWrite;
    private Connection main;
    private Connection replica;

    @Before
    public void resetState() {
        clock = MutableClock.epochUTC();
        lastWrite = new VolatileCache<>();
        main = null;
        replica = null;
    }

    @Test
    public void shouldBeConsistentAfterPropagation() {
        Duration maxPropagation = Duration.ofMillis(200);
        ReplicaConsistency consistency = new PessimisticPropagationConsistency(clock, maxPropagation, lastWrite);

        consistency.write(main);
        clock.add(Duration.ofMillis(300));
        boolean consistent = consistency.isConsistent(replica);

        assertThat(consistent).isTrue();
    }

    @Test
    public void shouldBeInconsistentBeforePropagation() {
        Duration maxPropagation = Duration.ofMillis(200);
        ReplicaConsistency consistency = new PessimisticPropagationConsistency(clock, maxPropagation, lastWrite);

        consistency.write(main);
        clock.add(Duration.ofMillis(50));
        boolean consistent = consistency.isConsistent(replica);

        assertThat(consistent).isFalse();
    }

    @Test
    public void shouldAssumeInconsistencyWhenUnknown() {
        Duration maxPropagation = Duration.ofMillis(200);
        ReplicaConsistency consistency = new PessimisticPropagationConsistency(clock, maxPropagation, lastWrite);

        clock.add(Duration.ofMillis(700));
        boolean consistent = consistency.isConsistent(replica);

        assertThat(consistent).isFalse();
    }
}
