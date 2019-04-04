/*
 * MetaDataCache.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaData;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * A caching hook for {@link FDBMetaDataStore}.
 * A cache implementation can implement any subset of object, serialized and version cached.
 * The caller is responsible for calling all of the relevant the <code>set</code> methods
 * when something changes.
 *
 * <p>
 * Note that this interface is currently undergoing active development. Work on evolving
 * this API is currently being tracked as part of
 * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/280">Issue #280</a>.
 * Users are advised to avoid implementing or using this interface until work on that issue
 * has been completed.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public interface MetaDataCache {
    /**
     * Get a version to use for cache validation of <code>-1</code> to skip validation.
     * @param context the transaction to use to access the database
     * @return a future that completes with the current version
     */
    default CompletableFuture<Integer> getCurrentVersionAsync(FDBRecordContext context) {
        return CompletableFuture.completedFuture(-1);

    }

    /**
     * Get cached {@link RecordMetaData} object. For instance, from a Guava cache.
     * @return any cached meta-data
     */
    @Nullable
    default RecordMetaData getCachedMetaData() {
        return null;
    }

    /**
     * Get cached serialized meta-data. For instance, from Memcache.
     * @return any cached serialized form
     */
    @Nullable
    default byte[] getCachedSerialized() {
        return null;
    }

    /**
     * Update the version used for cache validation.<p>
     * If maintained, the version should be stored in a way that is transactionally
     * consistent with the given <code>context</code>. For instance, in one or more
     * key-value pairs.
     * @param context the transaction to use to access the database
     * @param version the new current version
     * @see #getCurrentVersionAsync(FDBRecordContext)
     */
    default void setCurrentVersion(FDBRecordContext context, int version) {
    }

    /**
     * Cache meta-data object.<p>
     * This cache can be kept in memory. If {@link #setCurrentVersion} is supported, it is
     * still possible to determine when it is out-of-date, even if changed by some other
     * client.
     * @param metaData the new cached meta-data
     * @see #getCachedMetaData
     */
    default void setCachedMetaData(@Nonnull RecordMetaData metaData) {
    }

    /**
     * Cache serialized meta-data.<p>
     * This cache is normally maintained some place that is not transationally consistent.
     * In which case, if multiple clients can change the meta-data, {@link #setCurrentVersion}
     * should also be supported.
     * @param serialized the new cached serialized meta-data
     * @see #getCachedSerialized
     */
    default void setCachedSerialized(@Nonnull byte[] serialized) {
    }
}
