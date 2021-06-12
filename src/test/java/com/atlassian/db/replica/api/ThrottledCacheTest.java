package com.atlassian.db.replica.api;

import org.junit.jupiter.api.Test;

import java.util.ConcurrentModificationException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class ThrottledCacheTest {
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void staleWhileInvalidating() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>();

        cache.get(() -> 1L);
        asyncPutWithSlowSupplier(cache, anyValue());

        assertThat(cache.get(this::anyValue)).hasValue(1L);
    }


    @Test
    public void serveLatestValue() {
        ThrottledCache<Long> cache = new ThrottledCache<>();
        cache.get(this::anyValue);
        cache.get(this::anyValue);

        assertThat(cache.get(() -> 4L)).hasValue(4L);
    }

    @Test
    public void noConcurrentInvalidation() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>();

        asyncPutWithSlowSupplier(cache, anyValue());
        cache.get(() -> {
            throw new ConcurrentModificationException("Concurrent invalidation is not allowed");
        });

        assertThatNoException();
    }

    @Test
    public void emptyWhenLoadingFirstTime() throws InterruptedException {
        ThrottledCache<Long> cache = new ThrottledCache<>();

        asyncPutWithSlowSupplier(cache, anyValue());

        assertThat(cache.get(this::anyValue)).isEmpty();
    }


    private void asyncPutWithSlowSupplier(ThrottledCache<Long> cache, long value) throws InterruptedException {
        final CountDownLatch asyncThreadStarted = new CountDownLatch(1);
        executor.submit(() -> {
            cache.get(() -> {
                try {
                    asyncThreadStarted.countDown();
                    Thread.sleep(1000);
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
}
