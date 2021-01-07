/*
 * IndexTypes.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.FunctionNames;

/**
 * The standard index types.
 *
 * The type of an {@link Index} is just a string, so that new ones can be defined outside the Record Layer core.
 * @see com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistry
 */
@API(API.Status.MAINTAINED)
public class IndexTypes {

    /**
     * A normal index on field values.
     * The index is ordered, lexicographically if there is more than one field.
     */
    public static final String VALUE = "value";

    /* Atomic mutation indexes */
    // These types with the same name as functions need to match for the simpler cases in AtomicMutationIndexMaintainer.matchesAggregateFunction.

    /**
     * A count of the number of indexed entries.
     * If the index's expression is grouped, a separate count is maintained for each group.
     */
    public static final String COUNT = FunctionNames.COUNT;

    /**
     * A count of the number of times that any indexed record has been updated.
     * Unlike {@link #COUNT}, the count does not go down when records are deleted.
     */
    public static final String COUNT_UPDATES = FunctionNames.COUNT_UPDATES;

    /**
     * A count of the number of non-null values for a field.
     * If the index's expression has more than one field, the count indicates when <em>none</em> of the fields was null.
     */
    public static final String COUNT_NOT_NULL = FunctionNames.COUNT_NOT_NULL;

    /**
     * A total of a long-valued field.
     */
    public static final String SUM = FunctionNames.SUM;

    /**
     * Compatible name for {@link #MIN_EVER_LONG}.
     * @deprecated use {@link #MIN_EVER_LONG} for compatibility with existing data or {@link #MIN_EVER_TUPLE} for more flexibility
     */
    @Deprecated
    public static final String MIN_EVER = FunctionNames.MIN_EVER;

    /**
     * Compatible name for {@link #MAX_EVER_LONG}.
     * @deprecated use {@link #MAX_EVER_LONG} for compatibility with existing data or {@link #MAX_EVER_TUPLE} for more flexibility
     */
    @Deprecated
    public static final String MAX_EVER = FunctionNames.MAX_EVER;

    /**
     * An index remembering the least value(s) ever stored.
     * The order of values is the same as for keys: lexicographic among fields and each field ordered by {@link com.apple.foundationdb.tuple.Tuple#compareTo}.
     */
    public static final String MIN_EVER_TUPLE = FunctionNames.MIN_EVER + "_tuple";

    /**
     * An index remembering the greatest value(s) ever stored.
     */
    public static final String MAX_EVER_TUPLE = FunctionNames.MAX_EVER + "_tuple";

    /**
     * An index remembering the smallest nonnegative integer ever stored.
     * Because this index does not work with negative numbers or non-integer fields, consider using {@link #MIN_EVER_TUPLE} instead.
     */
    public static final String MIN_EVER_LONG = FunctionNames.MIN_EVER + "_long";

    /**
     * An index remembering the smallest nonnegative integer ever stored.
     * Because this index does not work with negative numbers or non-integer fields, consider using {@link #MAX_EVER_TUPLE} instead.
     */
    public static final String MAX_EVER_LONG = FunctionNames.MAX_EVER + "_long";

    /**
     * An index remembering the maximum ever value stored where one column is the
     * {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion record version}.
     * This can be used to detect if there have been any changes to the record store or to subsections
     * of the record store (if there are grouping columns included in the index definition).
     *
     * <p>
     * This index is like {@link #MAX_EVER_TUPLE} except that the expression may contain exactly
     * one aggregated column that contains a
     * {@link com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression VersionKeyExpression}
     * with one caveat. If a record is written with an incomplete version-stamp, then the aggregate index
     * entry is guaranteed to be updated to include the value from that record. For example, if there
     * are two columns defined as part of the index, and the first corresponds to an integer field while the
     * second corresponds to the record version, then the value stored by this index may go backwards
     * if, for example, one stores a record where the first field has value 2 and has an incomplete
     * version-stamp and then, in a separate transaction, one stores a record with the first field has
     * value 1 and has an incomplete version-stamp.
     * </p>
     */
    public static final String MAX_EVER_VERSION = FunctionNames.MAX_EVER + "_version";

    /**
     * A ranked set index, allowing efficient rank and select operations.
     */
    public static final String RANK = "rank";

    /**
     * An index with sliding time window leaderboards.
     */
    public static final String TIME_WINDOW_LEADERBOARD = "time_window_leaderboard";

    /**
     * An index like {@link #VALUE}, but that allows one of the components to be {@link com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression}.
     */
    public static final String VERSION = "version";

    /**
     * An index on the tokens in a text field.
     */
    public static final String TEXT = "text";

    /**
     * An index on the tokens in a text field.
     */
    public static final String FULL_TEXT = "full_text";

    /**
     * An index storing bitmaps of which records meet a specific condition.
     */
    public static final String BITMAP_VALUE = "bitmap_value";

    private IndexTypes() {
    }
}
