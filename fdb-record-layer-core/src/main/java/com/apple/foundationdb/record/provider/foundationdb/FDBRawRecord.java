/*
 * FDBRawRecord.java
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

import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A wrapper around all information that can be determined about a record before deserializing it.
 * In particular, this contains the record's primary key, its raw byte-string representation (after
 * any splits have been removed), and its version. It also includes sizing information describing
 * the record's on-disk footprint.
 */
public class FDBRawRecord implements FDBStoredSizes {
    @Nonnull private final Tuple primaryKey;
    @Nonnull private final byte[] rawRecord;
    @Nullable private final FDBRecordVersion version;

    // Size information
    private final int keyCount;
    private final int keySize;
    private final int valueSize;
    private final boolean split;
    private final boolean versionedInline;

    public FDBRawRecord(@Nonnull Tuple primaryKey, @Nonnull byte[] rawRecord, @Nullable FDBRecordVersion version,
                        @Nonnull FDBStoredSizes size) {
        this(primaryKey, rawRecord, version, size.getKeyCount(), size.getKeySize(), size.getValueSize(), size.isSplit(), size.isVersionedInline());
    }

    @SuppressWarnings("squid:S00107") // too many parameters
    public FDBRawRecord(@Nonnull Tuple primaryKey, @Nonnull byte[] rawRecord, @Nullable FDBRecordVersion version,
                        int keyCount, int keySize, int valueSize, boolean split, boolean versionedInline) {
        this.primaryKey = primaryKey;
        this.rawRecord = rawRecord;
        this.version = version;
        this.keyCount = keyCount;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.split = split;
        this.versionedInline = versionedInline;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FDBRawRecord that = (FDBRawRecord) o;
        if (!primaryKey.equals(that.getPrimaryKey())) {
            return false;
        }
        if (!Arrays.equals(rawRecord, that.getRawRecord())) {
            return false;
        }
        if (!Objects.equals(version, that.version)) {
            return false;
        }

        return this.keyCount == that.keyCount && this.keySize == that.keySize && this.valueSize == that.valueSize
               && this.split == that.split && this.versionedInline == that.versionedInline;
    }

    @Override
    public int hashCode() {
        return 31 * primaryKey.hashCode() + Arrays.hashCode(rawRecord) + (version != null ? 31 * 31 * version.hashCode() : 0);
    }

    /**
     * Get the primary key for this record.
     * @return the primary key for this record
     */
    @Nonnull
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Get the raw representation of the record. Note that this does
     * <i>not</i> make a copy for performance reasons, so any modifications
     * made to the returned array will also affect the array stored
     * within this object, which should generally be avoided
     *
     * @return the raw representation of this record
     */
    @Nonnull
    public byte[] getRawRecord() {
        return rawRecord;
    }

    /**
     * Get whether this record has a non-<code>null</code> version.
     * @return whether this record has a non-<code>null</code> version
     */
    public boolean hasVersion() {
        return version != null;
    }

    /**
     * Get the version associated with this record. This might be
     * <code>null</code>.
     * @return the version associated with this record
     */
    @Nullable
    public FDBRecordVersion getVersion() {
        return version;
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @Override
    public int getKeySize() {
        return keySize;
    }

    @Override
    public int getValueSize() {
        return valueSize;
    }

    @Override
    public boolean isSplit() {
        return split;
    }

    @Override
    public boolean isVersionedInline() {
        return versionedInline;
    }
}
