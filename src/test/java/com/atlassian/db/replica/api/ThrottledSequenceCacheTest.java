package com.atlassian.db.replica.api;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.catchThrowable;

public class ThrottledSequenceCacheTest {
    private final CacheLoader cacheLoader = new CacheLoader();

    private final TickingClock clock = new TickingClock();

    @Test
    public void staleWhileInvalidating() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofSeconds(60)
        ).sequenceCache(Long::compare).build();

        cache.get(() -> 1L);
        cacheLoader.asyncPutWithSlowSupplier(cache, anyValue());

        assertThat(cache.get(this::anyValue)).hasValue(1L);
    }

    @Test
    public void shouldAllowOnlyToIncreaseTheCachedValue() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofSeconds(60)
        ).sequenceCache(Long::compare).build();

        cache.get(() -> 1L);
        cache.get(() -> 2L);
        cache.get(() -> 3L);
        cache.get(() -> 2L);

        assertThat(cache.get(() -> 1L)).hasValue(3L);
    }

    @Test
    public void readsCachedValue() {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofSeconds(60)
        ).sequenceCache(Long::compare).build();

        cache.get(() -> 1L);

        assertThat(cache.get()).hasValue(1L);
    }

    @Test
    public void serveLatestValue() {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofSeconds(60)
        ).sequenceCache(Long::compare).build();
        cache.get(() -> 1L);
        cache.get(() -> 2L);

        assertThat(cache.get(() -> 4L)).hasValue(4L);
    }

    @Test
    public void noConcurrentInvalidation() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofSeconds(60)
        ).sequenceCache(Long::compare).build();

        cacheLoader.asyncPutWithSlowSupplier(cache, anyValue());
        cache.get(() -> {
            throw new ConcurrentModificationException("Concurrent invalidation is not allowed");
        });

        assertThatNoException();
    }

    @Test
    public void emptyWhenLoadingFirstTime() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofSeconds(60)
        ).sequenceCache(Long::compare).build();

        cacheLoader.asyncPutWithSlowSupplier(cache, anyValue());

        assertThat(cache.get(this::anyValue)).isEmpty();
    }

    @Test
    public void shouldSupplierBlockTheCacheUntilTimeout() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofMillis(1500)
        ).sequenceCache(Long::compare).build();

        cacheLoader.asyncPutWithSlowSupplier(cache, anyValue());

        clock.tick();

        assertThat(cache.get(this::anyValue)).isEmpty();
    }

    @Test
    public void shouldntSupplierBlockTheCacheForever() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofMillis(1500)
        ).sequenceCache(Long::compare).build();

        cacheLoader.asyncPutWithSlowSupplier(cache, anyValue());

        clock.tick();
        clock.tick();

        assertThat(cache.get(this::anyValue)).isNotEmpty();

    }

    @Test
    public void shouldNotRobbedThreadReleaseLock() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofMillis(1500)
        ).sequenceCache(Long::compare).build();

        final WaitingWork firstThreadWork = cacheLoader.asyncPutWithSlowSupplier(cache, 1);
        clock.tick();
        clock.tick();
        cacheLoader.asyncPutWithSlowSupplier(cache, 2);
        firstThreadWork.finish();

        final Throwable throwable = catchThrowable(() -> cache.get(() -> {
            throw new ConcurrentModificationException("Concurrent invalidation is not allowed");
        }));

        assertThat(throwable).doesNotThrowAnyException();
    }

    @Test
    public void shouldntRobbedThreadUpdateValue() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofMillis(1500)
        ).sequenceCache(Long::compare).build();

        final WaitingWork firstThreadWork = cacheLoader.asyncPutWithSlowSupplier(cache, 1);
        clock.tick();
        clock.tick();
        cache.get(() -> 2L);
        firstThreadWork.finish();
        assertThat(cache.get()).isEqualTo(Optional.of(2L));
    }

    @Test
    public void shouldFailingSupplierReleaseTheLock() {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofMillis(1500)
        ).sequenceCache(Long::compare).build();

        cache.get(() -> 1L);
        final Throwable throwable = catchThrowable(() -> {
            cache.get(() -> {
                throw new RuntimeException();
            });
        });

        assertThat(throwable).isNotNull();
        assertThat(cache.get()).isEqualTo(Optional.of(1L));
        assertThat(cache.get(() -> 2L)).isEqualTo(Optional.of(2L));
    }

    @Test
    public void shouldUpdateValuesWhenCacheLoadSlowerThanTimeout() throws InterruptedException {
        ThrottledCache<Long> cache = ThrottledCache.<Long>builder(
            clock,
            ofMillis(500)
        ).sequenceCache(Long::compare).build();

        final List<WaitingWork> waitingWorks = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            final WaitingWork waitingWork = cacheLoader.asyncPutWithSlowSupplier(cache, i);
            waitingWorks.add(waitingWork);
            clock.tick();
        }
        for (int i = 0; i < 32; i++) {
            waitingWorks.get(i).finish();
            assertThat(cache.get()).isEqualTo(Optional.of((long) i));
        }
    }

    private Long anyValue() {
        return new Random().nextLong();
    }

}
