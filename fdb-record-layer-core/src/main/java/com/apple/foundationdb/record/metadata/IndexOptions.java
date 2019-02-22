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

    private IndexOptions() {
    }
}
