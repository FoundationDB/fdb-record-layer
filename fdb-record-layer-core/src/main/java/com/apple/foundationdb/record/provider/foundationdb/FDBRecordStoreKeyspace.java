/*
 * FDBRecordStoreKeyspace.java
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
import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;

/**
 * Unique integers used as the first tuple item within a record store's subspace.
 */
@API(API.Status.UNSTABLE)
public enum FDBRecordStoreKeyspace {
    STORE_INFO(0L),
    RECORD(1L),
    INDEX(2L),
    INDEX_SECONDARY_SPACE(3L),
    RECORD_COUNT(4L),
    INDEX_STATE_SPACE(5L),
    INDEX_RANGE_SPACE(6L),
    INDEX_UNIQUENESS_VIOLATIONS_SPACE(7L),
    RECORD_VERSION_SPACE(8L),
    INDEX_BUILD_SPACE(9L),
    ;

    private long id;
    @Nonnull
    private Object key;

    FDBRecordStoreKeyspace(long id) {
        this.id  = id;
        this.key = id;
    }

    public long id() {
        return id;
    }

    @Nonnull
    public Object key() {
        return key;
    }

    public static FDBRecordStoreKeyspace fromKey(Object key) {
        if (key != null && key instanceof Long) {
            long id = ((Long) key).longValue();
            for (FDBRecordStoreKeyspace keySpace : FDBRecordStoreKeyspace.values()) {
                if (id == keySpace.id) {
                    return keySpace;
                }
            }
        }
        throw new RecordCoreException("Unrecognized keyspace: " + key);
    }
}
