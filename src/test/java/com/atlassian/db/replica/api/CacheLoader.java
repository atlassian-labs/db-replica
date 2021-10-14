package com.atlassian.db.replica.api;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheLoader {
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public WaitingWork asyncPutWithSlowSupplier(
        ThrottledCache<Long> cache,
        long value
    ) throws InterruptedException {
        final CountDownLatch asyncThreadStarted = new CountDownLatch(1);
        final CountDownLatch asyncThreadFinished = new CountDownLatch(1);
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
            asyncThreadFinished.countDown();
        });
        asyncThreadStarted.await();
        return new WaitingWork(threadWaiting, asyncThreadFinished);
    }
}
