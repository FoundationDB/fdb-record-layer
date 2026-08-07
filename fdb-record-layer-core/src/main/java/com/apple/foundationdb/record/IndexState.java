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
    READABLE_UNIQUE_PENDING(3L, "indexesReadableUniquePending"),
    /**
     * Similar to {@link #WRITE_ONLY}, but user updates are written
     * to a write pending queue rather than directly into the index.
     * Queries cannot use the index in this state.
     * This index state is designed to prevent conflicts between the online indexer and user io.
     */
    WRITE_ONLY_WITH_QUEUE(4L, "indexesWriteOnlyWithQueue");

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

    /**
     * Determine if an index in this state is readable.
     *
     * @return <code>true</code> if this state is {@link #READABLE} and <code>false</code> otherwise
     */
    public boolean isReadable() {
        return this.equals(READABLE);
    }

    /**
     * Determine if an index in this state is readable-unique-pending.
     * The readable-unique-pending index state may happen after a unique index is built, but duplications are
     * found. The index will be maintained in this mode until the last duplication is resolved, then its state
     * can be changed to {@link #READABLE}.
     *
     * @return <code>true</code> if this state is {@link #READABLE_UNIQUE_PENDING} and <code>false</code> otherwise
     */
    public boolean isReadableUniquePending() {
        return this.equals(READABLE_UNIQUE_PENDING);
    }

    /**
     * Determine if an index in this state is scannable - i.e. either {@link #isReadable()} or
     * {@link #isReadableUniquePending()}.
     *
     * @return <code>true</code> if this state is scannable and <code>false</code> otherwise
     */
    public boolean isScannable() {
        return isReadable() || isReadableUniquePending();
    }

    /**
     * Determine if an index in this state is write-only (but not write-only-with-queue).
     *
     * @return <code>true</code> if this state is {@link #WRITE_ONLY} and <code>false</code> otherwise
     */
    public boolean isWriteOnlyNoQueue() {
        return this.equals(WRITE_ONLY);
    }

    /**
     * Determine if an index in this state is write-only with a pending queue.
     *
     * @return <code>true</code> if this state is {@link #WRITE_ONLY_WITH_QUEUE} and <code>false</code> otherwise
     */
    public boolean isWriteOnlyWithQueue() {
        return this.equals(WRITE_ONLY_WITH_QUEUE);
    }

    /**
     * Determine if an index in this state is write-only in any form ({@link #WRITE_ONLY} or
     * {@link #WRITE_ONLY_WITH_QUEUE}).
     *
     * @return <code>true</code> if this state is write-only in any form and <code>false</code> otherwise
     */
    public boolean isWriteOnly() {
        return isWriteOnlyNoQueue() || isWriteOnlyWithQueue();
    }

    /**
     * Determine if an index in this state is disabled.
     *
     * @return <code>true</code> if this state is {@link #DISABLED} and <code>false</code> otherwise
     */
    public boolean isDisabled() {
        return this.equals(DISABLED);
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
