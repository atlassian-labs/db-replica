package com.atlassian.db.replica.api;

import java.util.concurrent.CountDownLatch;

final class WaitingWork {
    private final CountDownLatch threadWaiting;
    private final CountDownLatch asyncThreadFinished;

    WaitingWork(CountDownLatch threadWaiting, CountDownLatch asyncThreadFinished) {
        this.threadWaiting = threadWaiting;
        this.asyncThreadFinished = asyncThreadFinished;
    }

    public void finish() {
        threadWaiting.countDown();
        try {
            asyncThreadFinished.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
