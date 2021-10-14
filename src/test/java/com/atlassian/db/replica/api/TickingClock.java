package com.atlassian.db.replica.api;

import org.apache.commons.lang.NotImplementedException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static java.time.Duration.ofSeconds;

class TickingClock extends Clock {
    private Instant instant = Instant.now();

    public void tick() {
        instant = instant.plus(ofSeconds(1));
    }

    @Override
    public ZoneId getZone() {
        throw new NotImplementedException();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new NotImplementedException();
    }

    @Override
    public Instant instant() {
        return instant;
    }
}
