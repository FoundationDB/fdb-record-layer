package com.apple.foundationdb.record.lucene.codec;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The randomized testing framework that Lucene uses has checks for leaked threads, but FDB depends on the
 * network thread, and common thread pool, so we filter those out.
 */
public class FDBThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(final Thread t) {
        return t.getName().equals("fdb-network-thread") ||
               t.getName().startsWith("ForkJoinPool.commonPool");
    }
}
