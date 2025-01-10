/*
 * IndexState.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * Different states an index might be in within a given store.
 * These states might differ between record stores that have
 * otherwise identical meta-data.
 */
@API(API.Status.UNSTABLE)
public enum IndexState {
    /**
     * This is the default state for an index. It is
     * safe to read this index and safe to use it for
     * queries as the record store will maintain the
     * index as new writes come in and the index
     * has already been built.
     */
    READABLE(0L, "indexesReadable"),
    /**
     * Indicates the index should not be read from
     * but is written to. This is the state that
     * an index should be in while it is being
     * built but the build has not completed.
     * Queries cannot use the index, but as
     * records are added and removed, the index
     * is updated.
     */
    WRITE_ONLY(1L, "indexesWriteOnly"),
    /**
     * Indicates that this store does not
     * use this index. The index cannot service
     * reads or queries and it is not maintained
     * by the record store.
     */
    DISABLED(2L, "indexesDisabled"),
    /**
     * Indicates that this unique index is fully "indexed", but
     * some uniqueness violations still exist. This may happen
     * when the online indexer finds some duplicating records.
     * In this mode, it is safe to consider an index as {@link #READABLE}
     * for queries as long as uniqueness is not assumed.
     */
    READABLE_UNIQUE_PENDING(3L, "indexesReadableUniquePending");

    private final long id;
    private final String logName;
    @Nonnull private final Object code;

    IndexState(long id, String logName) {
        this.id = id;
        this.code = id;
        this.logName = logName;
    }

    /**
     * Value corresponding to the state. When
     * this state needs to be stored within a
     * record store, this can be used as the
     * code.
     * @return the code to serialize to serialize this state
     */
    @Nonnull
    public Object code() {
        return code;
    }

    public String getLogName() {
        return logName;
    }

    public boolean isScannable() {
        return this.equals(READABLE) || this.equals(READABLE_UNIQUE_PENDING);
    }

    public static IndexState fromCode(@Nonnull Object code) {
        for (IndexState state : IndexState.values()) {
            if (state.code().equals(code)) {
                return state;
            }
        }
        throw new RecordCoreStorageException("No IndexState found matching code " + code);
    }
}
