package com.atlassian.db.replica.api;

import org.apache.commons.lang.NotImplementedException;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ConcurrentModificationException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class ThrottledCacheTest {
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final TickingClock clock = new TickingClock();
    @Test
    public void staleWhileInvalidating() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofSeconds(60));

        cache.get(() -> 1L);
        asyncPutWithSlowSupplier(cache, anyValue());

        assertThat(cache.get(this::anyValue)).hasValue(1L);
    }

    @Test
    public void readsCachedValue() {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofSeconds(60));

        cache.get(() -> 1L);

        assertThat(cache.get()).hasValue(1L);
    }

    @Test
    public void serveLatestValue() {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofSeconds(60));
        cache.get(this::anyValue);
        cache.get(this::anyValue);

        assertThat(cache.get(() -> 4L)).hasValue(4L);
    }

    @Test
    public void noConcurrentInvalidation() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofSeconds(60));

        asyncPutWithSlowSupplier(cache, anyValue());
        cache.get(() -> {
            throw new ConcurrentModificationException("Concurrent invalidation is not allowed");
        });

        assertThatNoException();
    }

    @Test
    public void emptyWhenLoadingFirstTime() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofSeconds(60));

        asyncPutWithSlowSupplier(cache, anyValue());

        assertThat(cache.get(this::anyValue)).isEmpty();
    }

    @Test
    public void shouldSupplierBlockTheCacheUntilTimeout() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofMillis(1500));

        asyncPutWithSlowSupplier(cache, anyValue(), Duration.ofDays(365));

        clock.tick();

        assertThat(cache.get(this::anyValue)).isEmpty();
    }

    @Test
    public void shouldntSupplierBlockTheCacheForever() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofMillis(1500));

        asyncPutWithSlowSupplier(cache, anyValue(), Duration.ofDays(365));

        clock.tick();
        clock.tick();

        assertThat(cache.get(this::anyValue)).isNotEmpty();

    }

    private void asyncPutWithSlowSupplier(ThrottledCache<Long> cache, long value) throws InterruptedException {
        asyncPutWithSlowSupplier(cache, value, Duration.ofMillis(1000));
    }

    private void asyncPutWithSlowSupplier(
        ThrottledCache<Long> cache,
        long value,
        Duration duration
    ) throws InterruptedException {
        final CountDownLatch asyncThreadStarted = new CountDownLatch(1);
        executor.submit(() -> {
            cache.get(() -> {
                try {
                    asyncThreadStarted.countDown();
                    Thread.sleep(duration.toMillis());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return value;
            });
        });
        asyncThreadStarted.await();
    }

    private Long anyValue() {
        return new Random().nextLong();
    }

    private static class TickingClock extends Clock {
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
}
