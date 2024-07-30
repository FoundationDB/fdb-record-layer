/*
 * FunctionNames.java
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

/**
 * Names of core-supported query functions.
 */
@API(API.Status.MAINTAINED)
public class FunctionNames {

    /* Aggregate functions */
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String COUNT = "count";
    public static final String COUNT_UPDATES = "count_updates";
    public static final String COUNT_NOT_NULL = "count_not_null";
    public static final String COUNT_DISTINCT = "count_distinct";
    public static final String SUM = "sum";
    public static final String TIME_WINDOW_COUNT = "time_window_count";

    /* Aggregate index functions */
    public static final String MIN_EVER = "min_ever";
    public static final String MAX_EVER = "max_ever";

    /* Record functions */
    public static final String RANK = "rank";
    public static final String TIME_WINDOW_RANK = "time_window_rank";
    public static final String TIME_WINDOW_RANK_AND_ENTRY = "time_window_rank_and_entry";
    public static final String VERSION = "version";

    /* Score for rank functions */
    public static final String SCORE_FOR_RANK = "score_for_rank";
    public static final String SCORE_FOR_TIME_WINDOW_RANK = "score_for_time_window_rank";
    public static final String SCORE_FOR_RANK_ELSE_SKIP = "score_for_rank_else_skip";
    public static final String SCORE_FOR_TIME_WINDOW_RANK_ELSE_SKIP = "score_for_time_window_rank_else_skip";

    /* Rank for score (even if not present) functions */
    public static final String RANK_FOR_SCORE = "rank_for_score";
    public static final String TIME_WINDOW_RANK_FOR_SCORE = "time_window_rank_for_score";

    /* Bitmap of matching positions */
    public static final String BITMAP_VALUE = "bitmap_value";

    /* Arithmetic functions */
    public static final String ADD = "add";
    public static final String SUBTRACT = "subtract";
    public static final String MULTIPLY = "multiply";
    public static final String DIVIDE = "divide";
    public static final String MOD = "mod";
    public static final String BITAND = "bitand";
    public static final String BITOR = "bitor";
    public static final String BITXOR = "bitxor";
    public static final String BITNOT = "bitnot";

    private FunctionNames() {
    }
}
