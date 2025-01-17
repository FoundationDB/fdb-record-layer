/*
 * PassThroughRecordStoreStateCacheFactory.java
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
 * A factory for creating {@link PassThroughRecordStoreStateCache}s. That class is a singleton, but it is useful
 * to have a factory nonetheless so that one can be provided to the
 * {@link FDBDatabaseFactory FDBDatabaseFactory}.
 */
@API(API.Status.EXPERIMENTAL)
public class PassThroughRecordStoreStateCacheFactory implements FDBRecordStoreStateCacheFactory {
    private static final PassThroughRecordStoreStateCacheFactory INSTANCE = new PassThroughRecordStoreStateCacheFactory();

    private PassThroughRecordStoreStateCacheFactory() {
    }

    @Nonnull
    @Override
    public PassThroughRecordStoreStateCache getCache(@Nonnull FDBDatabase database) {
        // Parameter purposefully ignored. All databases can share the same instance.
        return PassThroughRecordStoreStateCache.instance();
    }

    /**
     * Get the factory singleton. Note that neither this factory nor the {@link PassThroughRecordStoreStateCache}
     * that it produces have any state.
     *
     * @return the factory's singleton
     */
    @Nonnull
    public static PassThroughRecordStoreStateCacheFactory instance() {
        return INSTANCE;
    }
}
