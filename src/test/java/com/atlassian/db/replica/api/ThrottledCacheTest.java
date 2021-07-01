package com.atlassian.db.replica.api;

import org.apache.commons.lang.NotImplementedException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
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
import static org.assertj.core.api.Assertions.catchThrowable;

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

        asyncPutWithSlowSupplier(cache, anyValue());

        clock.tick();

        assertThat(cache.get(this::anyValue)).isEmpty();
    }

    @Test
    public void shouldntSupplierBlockTheCacheForever() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofMillis(1500));

        asyncPutWithSlowSupplier(cache, anyValue());

        clock.tick();
        clock.tick();

        assertThat(cache.get(this::anyValue)).isNotEmpty();

    }

    @Test
    public void shouldNotRobbedThreadReleaseLock() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>(clock, ofMillis(1500));

        final WaitingWork firstThreadWork = asyncPutWithSlowSupplier(cache, 1);
        clock.tick();
        clock.tick();
        asyncPutWithSlowSupplier(cache, 2);
        firstThreadWork.finish();

        final Throwable throwable = catchThrowable(() -> cache.get(() -> {
            throw new ConcurrentModificationException("Concurrent invalidation is not allowed");
        }));

        assertThat(throwable).doesNotThrowAnyException();
    }

    private WaitingWork asyncPutWithSlowSupplier(
        ThrottledCache<Long> cache,
        long value
    ) throws InterruptedException {
        final CountDownLatch asyncThreadStarted = new CountDownLatch(1);
        final CountDownLatch threadWaiting = new CountDownLatch(1);
        executor.submit(() -> {
            cache.get(() -> {
                asyncThreadStarted.countDown();
                try {
                    threadWaiting.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return value;
            });
        });
        asyncThreadStarted.await();
        return new WaitingWork(threadWaiting);
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

    private final static class WaitingWork {
        private final CountDownLatch latch;

        private WaitingWork(CountDownLatch latch) {
            this.latch = latch;
        }

        public void finish() throws InterruptedException {
            latch.countDown();
        }
    }
}
