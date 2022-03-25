package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.MoreAsyncUtil;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@API(API.Status.INTERNAL)
public class ExponentialDelay {
    // TODO write some tests for this, namely that it trends towards maxDelayMillis, rather than minDelayMillis
    private static final long MIN_DELAY_MILLIS = 2;
    private long currentDelayMillis;
    private long maxDelayMillis;
    private long nextDelayMillis;

    public ExponentialDelay(final long initialDelayMillis, final long maxDelayMillis) {
        currentDelayMillis = initialDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
    }

    public CompletableFuture<Void> delay() {
        return MoreAsyncUtil.delayedFuture(nextDelayMillis, TimeUnit.MILLISECONDS)
                .thenApply(vignore -> {
                    calculateNextDelay();
                    return vignore;
                });
    }

    protected void calculateNextDelay() {
        currentDelayMillis = Math.max(Math.min(currentDelayMillis * 2, maxDelayMillis), MIN_DELAY_MILLIS);
        nextDelayMillis = (long)(Math.random() * currentDelayMillis);
    }

    public long getNextDelayMillis() {
        return nextDelayMillis;
    }
}
