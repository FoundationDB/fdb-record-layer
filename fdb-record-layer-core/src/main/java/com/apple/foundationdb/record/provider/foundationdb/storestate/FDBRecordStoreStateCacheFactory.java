/*
 * FDBRecordStoreStateCacheFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.storestate;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;

import javax.annotation.Nonnull;

/**
 * A factory interface for {@link FDBRecordStoreStateCache}s. This can be given to the
 * {@link FDBDatabaseFactory FDBDatabaseFactory}
 * singleton to ensure that each
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase FDBDatabase} is provided with its own cache
 * instance.
 *
 * @see FDBRecordStoreStateCache
 * @see FDBDatabaseFactory#setStoreStateCacheFactory(FDBRecordStoreStateCacheFactory)
 */
@API(API.Status.EXPERIMENTAL)
public interface FDBRecordStoreStateCacheFactory {
    /**
     * Produce a {@link FDBRecordStoreStateCache}. Two instances produced by this factory should not
     * share any common state as they should not be assumed to cache store information for record stores
     * from the same database.
     *
     * @param database the database that the produced cache will be used with
     * @return a {@link FDBRecordStoreStateCache}
     */
    @Nonnull
    FDBRecordStoreStateCache getCache(@Nonnull FDBDatabase database);
}
