/*
 * IndexEntry.java
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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.Key.Evaluated.NullStandin;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An <code>IndexEntry</code> carries around the key and value read from an index (as the name would imply).
 * Further, if the key and value were produced by applying an index key expression to a record, this will carry
 * around additional information about the nulls contained in the expression.
 */
public class IndexEntry {
    private static final NullStandin[] NO_NULLS = new NullStandin[0];

    @Nonnull
    private final Tuple key;
    @Nonnull
    private final Tuple value;

    // This will be null if created from a tuple, in which case it is not legal to ask the question about
    // the type of null at a given position. If it is zero length, then it came from a Key.Evaluated but
    // did not contain any null values.
    @Nullable
    private NullStandin[] nullStandins;

    public IndexEntry(@Nonnull Key.Evaluated key, @Nonnull Key.Evaluated value) {
        this(key.toTuple(), value.toTuple());
        int idx = 0;
        for (Object keyValue : key.values()) {
            if (keyValue instanceof NullStandin) {
                if (nullStandins == null) {
                    nullStandins = new NullStandin[key.size()];
                }
                nullStandins[idx] = (NullStandin) keyValue;
            }
            ++idx;
        }
        if (nullStandins == null) {
            nullStandins = NO_NULLS;
        }
    }

    public IndexEntry(@Nonnull Key.Evaluated key) {
        this(key, Key.Evaluated.EMPTY);
    }

    public IndexEntry(@Nonnull Tuple key, @Nonnull Tuple value) {
        this.key = key;
        this.value = value;
    }

    @Nonnull
    public Tuple getValue() {
        return value;
    }

    @Nonnull
    public Tuple getKey() {
        return key;
    }

    public int getKeySize() {
        return key.size();
    }

    /**
     * Get a tuple element of the key tuple.
     * @param idx the index of the desired element
     * @return the tuple element at index {@code idx}
     */
    @Nullable
    public Object getKeyValue(int idx) {
        return key.get(idx);
    }

    /**
     * Returns true if the key expression contains a NULL value that is not a {@link NullStandin#NULL_UNIQUE} value.
     * Calling this method on a <code>IndexEntry</code> that was created directly from tuples will result in an
     * exception.
     * @return {@code true} if the key contains a non-unique null value
     */
    public boolean keyContainsNonUniqueNull() {
        checkIfNullTypeAvailable();
        for (NullStandin nullStandin : nullStandins) {
            if (nullStandin == NullStandin.NULL) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the type of null stored in a given key index location. Calling this method on
     * an <code>IndexEntry</code> that was created directly from tuples, or if the value at <code>idx</code>
     * is not null, will result in an exception.
     * @param idx the index of a null element
     * @return the type of null stored at {@code idx}
     */
    @Nonnull
    public NullStandin getKeyNullType(int idx) {
        checkIfNullTypeAvailable();
        if (nullStandins.length == 0 || nullStandins[idx] == null) {
            throw new RecordCoreException("Value is not null").addLogInfo("index", idx);
        }
        return nullStandins[idx];
    }

    /**
     * Produces a new <code>IndexEntry</code> whose key is a subset of this <code>IndexEntry</code>.
     * @param startIdx the starting offset of the key to use for the new value
     * @param endIdx the ending offset (exclusive) of the key to use for the new value
     * @return a new index entry with the subset key between {@code startIdx} and {@code endIdx}
     */
    @Nonnull
    public IndexEntry subKey(int startIdx, int endIdx) {
        if (startIdx == 0 && endIdx == key.size()) {
            return this;
        }

        IndexEntry subKey = new IndexEntry(TupleHelpers.subTuple(key, startIdx, endIdx), value);
        if (nullStandins == null || nullStandins.length == 0) {
            subKey.nullStandins = nullStandins;
        } else {
            subKey.nullStandins = new NullStandin[endIdx - startIdx];
            System.arraycopy(nullStandins, startIdx, subKey.nullStandins, 0, endIdx - startIdx);
        }
        return subKey;
    }

    private void checkIfNullTypeAvailable() {
        // This indicates that the key/value was created from a tuple (i.e. likely values were read from an
        // index entry in the database) and, therefore, we don't know what type of null it was when it was
        // produced.  Basically this is an IllegalStateException.
        if (nullStandins == null) {
            throw new RecordCoreException("Type of null cannot be determined from tuple");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexEntry that = (IndexEntry) o;

        // It is important to use compare() here. Tuple.equals() packs the value which explodes if
        // the tuple contains an incomplete version stamp.
        return TupleHelpers.compare(this.key, that.key) == 0
                && TupleHelpers.compare(this.value, that.value) == 0;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return key + ":" + value;
    }
}
