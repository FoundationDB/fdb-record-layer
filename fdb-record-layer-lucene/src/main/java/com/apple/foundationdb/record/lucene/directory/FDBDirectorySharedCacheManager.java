/*
 * FDBDirectorySharedCacheManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cache for {@link FDBDirectory} blocks that can be shared between record contexts.
 *
 * The scope is the record store. Each directory cache is keyed by the index subspace's suffix relative to the record store plus
 * any grouping key.
 * Additionally, the cache is per directory sequence number. Any transaction that changes the directory must also advance this
 * counter. Only the latest sequence number's cache is retained by this manager, although it is possible that multiple older
 * transactions continue to share one.
 */
@API(API.Status.EXPERIMENTAL)
@ThreadSafe
public class FDBDirectorySharedCacheManager {
    public static final Object SHARED_CACHE_CONTEXT_KEY = new Object();
    @Nonnull
    private final Map<Tuple, FDBDirectorySharedCache> caches;
    @Nullable
    private final Subspace subspace;
    private final int maximumSize;
    private final int concurrencyLevel;
    private final int initialCapacity;

    @Nullable
    public Subspace getSubspace() {
        return subspace;
    }

    public int getMaximumSize() {
        return maximumSize;
    }

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public int getInitialCapacity() {
        return initialCapacity;
    }

    /**
     * Get any shared cache manager for the given context.
     *
     * Note that attaching the cache manager to the context means that the context cannot perform Lucene queries against
     * multiple record stores.
     * @param context the record context in which to find the shared cache manager
     * @return the shared cache manager set in the context or {@code null} if none has been set
     * @see #setForContext
     */
    @Nullable
    public static FDBDirectorySharedCacheManager forContext(@Nonnull FDBRecordContext context) {
        return context.getInSession(SHARED_CACHE_CONTEXT_KEY, FDBDirectorySharedCacheManager.class);
    }

    /**
     * Set the given shared cache manager in the given context.
     * @param context the record context in which to put the shared cache manager
     */
    public void setForContext(@Nonnull FDBRecordContext context) {
        context.putInSessionIfAbsent(SHARED_CACHE_CONTEXT_KEY, this);
    }

    protected FDBDirectorySharedCacheManager(@Nullable Subspace subspace, int maximumSize, int concurrencyLevel, int initialCapacity) {
        this.subspace = subspace;
        this.maximumSize = maximumSize;
        this.concurrencyLevel = concurrencyLevel;
        this.initialCapacity = initialCapacity;
        this.caches = new ConcurrentHashMap<>();
    }

    /**
     * Get a cache for a directory.
     * @param key the directory key, including the index prefix and any grouping keys
     * @param sequenceNumber the sequence number of the directory as read in the current transaction
     * @return a shared cache of {@code null} if the sequence number is too old
     */
    @Nullable
    public FDBDirectorySharedCache getCache(@Nonnull Tuple key, long sequenceNumber) {
        FDBDirectorySharedCache storedCache = caches.compute(key, (ckey, cache) -> {
            if (cache == null || cache.getSequenceNumber() < sequenceNumber) {
                cache = new FDBDirectorySharedCache(ckey, sequenceNumber,
                        maximumSize, concurrencyLevel, initialCapacity);
            }
            return cache;
        });
        if (storedCache.getSequenceNumber() == sequenceNumber) {
            return storedCache;
        }
        // Asking for a sequence number that is older than the current one. No shared cache.
        return null;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@code FDBDirectorySharedCacheManager}.
     */
    public static class Builder {
        private Subspace subspace;
        private int maximumSize = 1024;
        private int concurrencyLevel = 16;
        private int initialCapacity = 128;

        protected Builder() {
        }

        public Builder setSubspace(final Subspace subspace) {
            this.subspace = subspace;
            return this;
        }

        public Builder setMaximumSize(final int maximumSize) {
            this.maximumSize = maximumSize;
            return this;
        }

        public Builder setConcurrencyLevel(final int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        public Builder setInitialCapacity(final int initialCapacity) {
            this.initialCapacity = initialCapacity;
            return this;
        }

        public FDBDirectorySharedCacheManager build() {
            return new FDBDirectorySharedCacheManager(subspace, maximumSize, concurrencyLevel, initialCapacity);
        }
    }
}
