/*
 * FDBRecordCache.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A cache used to track and handle the results of asynchronously preloaded records.
 */
@API(API.Status.INTERNAL)
class FDBPreloadRecordCache {
    @Nonnull
    private final Cache<Tuple, EntryImpl>  cache;

    public FDBPreloadRecordCache(int maximumSize) {
        cache = CacheBuilder.newBuilder().maximumSize(maximumSize).build();
    }

    /**
     * Called prior to performing a prefetch for a given {@code Tuple}, returning a future holder that must
     * be filled in with the record when it has completed.
     *
     * @param tuple the tuple to be have prefetched
     * @return a holder with which to set the fetched value when it has completed
     */
    @Nonnull
    public Future beginPrefetch(@Nonnull Tuple tuple) {
        EntryImpl entry = new EntryImpl(tuple);
        cache.put(tuple, entry);
        return entry;
    }

    /**
     * Invalidates an entry in the cache, or discarding the outstanding result of a pre-fetch when it completes.
     * @param tuple the tuple to invalidate
     */
    public void invalidate(@Nonnull Tuple tuple) {
        cache.invalidate(tuple);
    }

    /**
     * Clears all entries from the cache. All pending pre-load requests will be abandoned.
     */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /**
     * Fetch an entry from the cache. A return value of {@code null} indicates that no cached entry is available to
     * be returned.  This may happen if no prefetch request was performed, the prefetch has not yet been completed,
     * or the prefetch was invalidated.
     *
     * @param tuple the tuple to fetch from the cache
     * @return the cached entry or {@code null} if not entry is available in the cache
     */
    @Nullable
    public Entry get(@Nonnull Tuple tuple) {
        EntryImpl entry = cache.getIfPresent(tuple);
        if (entry != null && entry.isComplete()) {
            return entry;
        }
        return null;
    }

    /**
     * An entry returned from the cache.
     */
    public interface Entry {
        boolean isPresent();

        FDBRawRecord get();

        @Nullable
        FDBRawRecord orElse(@Nullable FDBRawRecord other);
    }

    /**
     * A holder that can be used to record a completed prefetch operation.
     */
    public interface Future {
        /**
         * To be called to complete the future with the result of the prefetch operation.
         *
         * @param record the record that was preloaded or {@code null} if the record does not exist.
         */
        void complete(@Nullable FDBRawRecord record);

        /**
         * To be called if the attempt to preload the record failed, invalidating the future.
         */
        void cancel();
    }

    /**
     * The internal representation of a cache entry.  A cache entry is added before a record preload is
     * attempted, and is marked as not yet being completed. It is the completion of the record read that
     * will complete this entry, making its value visible in the cache. When an {@code Entry} is invalidated,
     * it is simply removed from the cache and, thus, even if the pending read of the record eventually
     * completes the {@code Entry}, it will not be visible within the cache.
     */
    private class EntryImpl implements Entry, Future {
        private boolean isComplete;

        @Nullable
        private FDBRawRecord rawRecord;

        @Nonnull
        private final Tuple primaryKey;

        public EntryImpl(@Nonnull Tuple primaryKey) {
            this.primaryKey = primaryKey;
        }

        @Override
        public void cancel() {
            cache.invalidate(primaryKey);
        }

        @Override
        public synchronized void complete(@Nullable final FDBRawRecord rawRecord) {
            isComplete = true;
            this.rawRecord = rawRecord;
        }

        private synchronized boolean isComplete() {
            return isComplete;
        }

        // An Entry is never made visible outside of the cache until it is completed, at which point its
        // contents are effectively immutable, so there is no need for synchronization on the methods below.

        @Override
        public boolean isPresent() {
            return rawRecord != null;
        }

        @Override
        public FDBRawRecord get() {
            if (rawRecord == null) {
                throw new IllegalStateException("record is null");
            }
            return rawRecord;
        }

        @Override
        @Nullable
        public FDBRawRecord orElse(@Nullable final FDBRawRecord other) {
            return rawRecord != null ? rawRecord : other;
        }
    }
}
