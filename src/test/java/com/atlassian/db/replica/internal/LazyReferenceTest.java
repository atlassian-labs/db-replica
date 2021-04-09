package com.atlassian.db.replica.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;


public class LazyReferenceTest {

    @Test
    public void shouldCreateValueOnce() {
        final CountingReference countingReference = new CountingReference(false);

        countingReference.get();
        countingReference.get();
        countingReference.get();

        assertThat(countingReference.getCounter()).isEqualTo(1);
    }

    @Test
    public void shouldCreateValueOnceWhileAccessedConcurrently() throws InterruptedException {
        final CountingReference countingReference = new CountingReference(false);
        final int threads = 128;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(threads);
        final CountDownLatch end = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> fetchReference(countingReference, start, end));
        }
        end.await();

        assertThat(countingReference.getCounter()).isEqualTo(1);
        executor.shutdown();
    }

    private void fetchReference(CountingReference countingReference, CountDownLatch start, CountDownLatch end) {
        start.countDown();
        try {
            start.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        countingReference.get();
        end.countDown();
    }


    private static class CountingReference extends LazyReference<Integer> {
        final AtomicInteger counter = new AtomicInteger();

        private CountingReference(boolean compatibleWithPreviousVersion) {
            super(compatibleWithPreviousVersion);
        }

        @Override
        protected Integer create() {
            return counter.incrementAndGet();
        }

        public int getCounter() {
            return counter.get();
        }
    }

}
