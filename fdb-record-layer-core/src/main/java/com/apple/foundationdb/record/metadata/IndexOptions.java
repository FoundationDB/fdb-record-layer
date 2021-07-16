/*
 * IndexOptions.java
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

import java.util.Collections;
import java.util.Map;

/**
 * The standard options for use with {@link Index}.
 *
 * An option key is just a string, so that new ones can be defined outside the Record Layer core.
 *
 * @see Index#getOptions
 */
@API(API.Status.MAINTAINED)
public class IndexOptions {

    /**
     * No options.
     *
     * The default for a new {@link Index}.
     */
    public static final Map<String, String> EMPTY_OPTIONS = Collections.emptyMap();

    /**
     * If {@code "true"}, index throws {@link com.apple.foundationdb.record.RecordIndexUniquenessViolation} on attempts to store duplicate values.
     */
    public static final String UNIQUE_OPTION = "unique";
    /**
     * Options to set to enable {@link #UNIQUE_OPTION}.
     */
    public static final Map<String, String> UNIQUE_OPTIONS = Collections.singletonMap(UNIQUE_OPTION, Boolean.TRUE.toString());

    /**
     * If {@code "false"}, the index is not considered for use in queries, even if enabled, unless requested explicitly.
     *
     * @see com.apple.foundationdb.record.query.RecordQuery.Builder#setAllowedIndexes
     */
    public static final String ALLOWED_FOR_QUERY_OPTION = "allowedForQuery";
    /**
     * Options to set to disable {@link #ALLOWED_FOR_QUERY_OPTION}.
     */
    public static final Map<String, String> NOT_ALLOWED_FOR_QUERY_OPTIONS = Collections.singletonMap(ALLOWED_FOR_QUERY_OPTION, Boolean.FALSE.toString());

    /**
     * Option indicating one (or more) indexes that replace this index. If this index is replaced by multiple indexes,
     * then multiple options can be specified by setting multiple options prefixed by this value. The value of each
     * option should be the name of another index in the meta-data. If for a given
     * {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore record store}, every index so
     * specified has been built, then the index with this option will have its data deleted and will be marked as
     * {@linkplain com.apple.foundationdb.record.IndexState#DISABLED disabled}.
     *
     * <p>
     * Every index in the list should point to another index in the meta-data. Additionally, the graph of replacement
     * indexes should be acyclic, i.e., indexes cannot be marked as replacing themselves either directly or
     * transitively. If either requirement is not met, then the index will fail {@linkplain MetaDataValidator
     * meta-data validation}
     * </p>
     *
     * <p>
     * This option is useful as it allows the user to replace one index with another without needing to remove the
     * older index until the newer index has fully rolled out. For example, suppose there is an index {@code a} that
     * is defined on a single field, but the administrator realizes that as all queries have predicates on at least two,
     * and so wants to replace the index with two indexes, {@code a,b} and {@code a,c}. However, until those new indexes
     * have been built, it is preferable to serve queries using the {@code a} index rather than to fallback to a costly
     * full record store scan. So the options {@code {"replacedBy1": "a,b", "replacedBy2": "a,c"}} can be set on the
     * index. Until both indexes have been built, queries can continue to use index {@code a}. Once both have been
     * built, though, the old index will be automatically deleted, freeing up space.
     * </p>
     *
     * <p>
     * Note that different record stores with identical
     * {@link com.apple.foundationdb.record.RecordMetaData RecordMetaData} values may have different index states, i.e.,
     * an index may be built for some record stores but not others. Setting this option will disable the index only on
     * each store that meets the necessary criteria.
     * </p>
     *
     * @see Index#getReplacedByIndexNames()
     */
    public static final String REPLACED_BY_OPTION_PREFIX = "replacedBy";

    /**
     * The name of the {@link com.apple.foundationdb.record.provider.common.text.TextTokenizer} to use with a {@link IndexTypes#TEXT} index.
     */
    public static final String TEXT_TOKENIZER_NAME_OPTION = "textTokenizerName";
    /**
     * The version of the {@link com.apple.foundationdb.record.provider.common.text.TextTokenizer} to use with a {@link IndexTypes#TEXT} index.
     */
    public static final String TEXT_TOKENIZER_VERSION_OPTION = "textTokenizerVersion";
    /**
     * If {@code "true"}, a {@link IndexTypes#TEXT} index will add a conflict range for the whole index to keep the commit size down at the expense of more conflicts.
     */
    @API(API.Status.EXPERIMENTAL)
    public static final String TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION = "textAddAggressiveConflictRanges";
    /**
     * If {@code "true"}, a {@link IndexTypes#TEXT} index will not store position numbers for tokens.
     *
     * It will only be possible to determine that an indexed field contains the token someplace.
     */
    public static final String TEXT_OMIT_POSITIONS_OPTION = "textOmitPositions";

    /**
     * The number of levels in the {@link IndexTypes#RANK} skip list {@link com.apple.foundationdb.async.RankedSet}.
     *
     * The default is {@link com.apple.foundationdb.async.RankedSet#DEFAULT_LEVELS} = {@value com.apple.foundationdb.async.RankedSet#DEFAULT_LEVELS}.
     */
    public static final String RANK_NLEVELS = "rankNLevels";

    /**
     * The hash function to use in the {@link IndexTypes#RANK} skip list {@link com.apple.foundationdb.async.RankedSet}.
     *
     * The default is {@link com.apple.foundationdb.async.RankedSet#DEFAULT_HASH_FUNCTION}.
     */
    public static final String RANK_HASH_FUNCTION = "rankHashFunction";

    /**
     * Whether duplicate keys count separtely in the {@link IndexTypes#RANK} skip list {@link com.apple.foundationdb.async.RankedSet}.
     *
     * The default is {@code false}.
     */
    public static final String RANK_COUNT_DUPLICATES = "rankCountDuplicates";

    /**
     * Size of each position bitmap for {@link IndexTypes#BITMAP_VALUE} indexes.
     *
     * The default is {@code 10,000}.
     */
    public static final String BITMAP_VALUE_ENTRY_SIZE_OPTION = "bitmapValueEntrySize";

    /**
     * Whether to remove index entry for {@link IndexTypes#COUNT} type indexes when they decrement to zero.
     *
     * This makes the existence of zero-valued entries in the index in the face of updates and deletes
     * closer to what it would be if the index were rebuilt, but still not always the same.
     * In particular,<ul>
     *   <li>A {@code SUM} index will not have entries for groups all of whose indexed values are zero.</li>
     *   <li>Changing the option for an existing index from {@code false} to {@code true} does not clear any entries.</li>
     * </ul>
     */
    public static final String CLEAR_WHEN_ZERO = "clearWhenZero";

    /**
     * Size of the portion of the grouping keys enumerated after the extrema by {@link IndexTypes#PERMUTED_MIN} and {@link IndexTypes#PERMUTED_MAX} indexes.
     */
    public static final String PERMUTED_SIZE_OPTION = "permutedSize";

    private IndexOptions() {
    }
}
