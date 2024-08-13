/*
 * LogMessageKeys.java
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

package com.apple.foundationdb.record.logging;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Common {@link KeyValueLogMessage} keys logged by the Record Layer core.
 * In general, we try to consolidate all of the keys here, so that it's easy to check for collisions, ensure consistency, etc.
 */
@API(API.Status.UNSTABLE)
public enum LogMessageKeys {
    // general keys
    TITLE("ttl"),
    CLUSTER,
    SUBSPACE,
    SUBSPACE_KEY,
    INDEX_SUBSPACE,
    CALLING_CLASS,
    CALLING_METHOD,
    CALLING_LINE,
    FUTURE_COMPLETED,
    SCAN_PROPERTIES,
    SCAN_TYPE,
    READ_VERSION,
    OLD,
    NEW,
    MESSAGE,
    CODE,
    DESCRIPTION,
    UNKNOWN_FIELDS,
    CURR_ATTEMPT,
    MAX_ATTEMPTS,
    DELAY,
    COMMIT_NAME,
    TRANSACTION_ID,
    TRANSACTION_NAME,
    AGE_SECONDS,
    CONSTITUENT,
    TOTAL_MICROS,
    // record splitting/unsplitting
    KEY,
    KEY_TUPLE,
    EXPECTED_INDEX,
    FOUND_INDEX,
    KNOWN_LAST_KEY,
    SPLIT_NEXT_INDEX("next_index"),
    SPLIT_REVERSE("reverse"),
    READ_LAST_KEY_MICROS,
    // protobuf parsing
    RAW_BYTES,
    // key space API keys
    PARENT_DIR,
    DIR_NAME,
    DIR_TYPE,
    DIR_VALUE,
    PROVIDED_KEY,
    PROVIDED_VALUE,
    LIST_FROM,
    SUBDIRECTORY,
    RANGE,
    RANGE_VALUE_TYPE,
    EXPECTED_VALUE_TYPE,
    ENDPOINT_TYPE,
    // stored size info
    KEY_COUNT,
    KEY_SIZE,
    VALUE_SIZE,
    IS_SPLIT,
    IS_VERSIONED_INLINE,
    // key expressions
    EXPECTED_COLUMN_SIZE,
    ACTUAL_COLUMN_SIZE,
    KEY_EXPRESSION,
    KEY_EVALUATED,
    // manipulating subkeys of key expressions
    REQUESTED_START,
    REQUESTED_END,
    COLUMN_SIZE,
    // query components
    FILTER,
    PARENT_FILTER,
    CHILD_FILTER,
    OTHER_FILTER,
    // Boolean normalization
    DNF_SIZE,
    DNF_SIZE_LIMIT,
    // index-related keys
    INDEX_NAME,
    INDEX_STATE,
    TARGET_INDEX_NAME,
    TARGET_INDEX_STATE,
    INDEX_MERGES_LAST_LIMIT,
    INDEX_MERGES_LAST_FOUND,
    INDEX_MERGES_LAST_TRIED,
    INDEX_MERGES_NUM_COMPLETED,
    INDEX_MERGES_CONTEXT_TIME_QUOTA,
    INDEX_MERGE_LOCK,
    INDEX_REPARTITION_DOCUMENT_COUNT,
    INDEX_DEFERRED_ACTION_STEP,
    AGILITY_CONTEXT,
    AGILITY_CONTEXT_AGE_MILLISECONDS,
    AGILITY_CONTEXT_PREV_CHECK_MILLISECONDS,
    AGILITY_CONTEXT_WRITE_SIZE_BYTES,
    PRIMARY_INDEX,
    VALUE_KEY,
    PRIMARY_KEY,
    VALUE,
    INDEX_OPERATION("operation"),
    INITIAL_PREFIX,
    SECOND_PREFIX,
    INDEX_VERSION,
    START_TUPLE,
    END_TUPLE,
    REAL_END,
    RECORDS_SCANNED,
    ORIGINAL_RANGE,
    SPLIT_RANGES,
    REASON,
    LATEST_ENTRY_TIMESTAMP,
    EARLIEST_ADDED_START_TIMESTAMP,
    REMOVE,
    TEXT_SIZE,
    UNIQUE_TOKENS,
    AVG_TOKEN_SIZE,
    MAX_TOKEN_SIZE,
    AVG_POSITIONS,
    MAX_POSITIONS,
    TEXT_KEY_SIZE,
    TEXT_VALUE_SIZE,
    TEXT_INDEX_SIZE_AMORTIZED,
    WIDENED_TUPLE_RANGE,
    WROTE_INDEX,
    NEW_STORE,
    RECORDS_WHILE_BUILDING,
    RECORDS_PER_SECOND,
    DOCUMENT,
    SESSION_ID,
    INDEXER_SESSION_ID,
    INDEXER_ID,
    INDEX_STATE_PRECONDITION,
    INITIAL_INDEX_STATE,
    SHOULD_BUILD_INDEX("do_build_index"),
    SHOULD_CLEAR_EXISTING_DATA("clear_existing_data"),
    SHOULD_MARK_READABLE("should_mark_readable"),
    RESULT,
    INDEXER_CURR_RETRY,
    INDEXER_MAX_RETRIES,
    /**
     * Identical to {@link #DIRECTORY}. This is included only for backwards compatibility.
     * @deprecated use {@link #DIRECTORY} instead
     */
    @Deprecated
    DIRECTOY("directory"),
    DIRECTORY,
    SOURCE_INDEX,
    SOURCE_FILE,

    // indexing
    CONTINUED_BUILD,
    INDEXING_METHOD,
    ALLOW_REPAIR,
    INDEXING_POLICY,
    INDEXING_POLICY_DESIRED_ACTION,
    INDEXING_FRAGMENTATION_COUNT,
    INDEXING_FRAGMENTATION_STEP,
    INDEXING_FRAGMENTATION_FIRST,
    INDEXING_FRAGMENTATION_TYPE,
    INDEXING_FRAGMENTATION_CURRENT,
    MISSING_RANGES,

    // FDB client configuration
    API_VERSION,
    RUN_LOOP_PROFILING,
    THREADS_PER_CLIENT_VERSION,
    TRACE_DIRECTORY,
    TRACE_FORMAT,
    TRACE_LOG_GROUP,
    UNCLOSED_WARNING,

    // comparisons
    COMPARISON_TYPE,
    COMPARISON_VALUE,
    EXPECTED,
    ACTUAL,
    // functional index keys
    FUNCTION,
    INDEX_KEY,
    KEY_SPACE_PATH,
    INDEX_VALUE,
    ARGUMENT_INDEX,
    EVALUATED_SIZE,
    EVALUATED_INDEX,
    EXPECTED_TYPE,
    ACTUAL_TYPE,
    // cursors
    CHILD_COUNT,
    EXPECTED_CHILD_COUNT,
    READ_CHILD_COUNT,
    TIME_STARTED,
    TIME_ENDED,
    DURATION_MILLIS,
    CURSOR_ELAPSED_MILLIS,
    CURSOR_TIME_LIMIT_MILLIS,
    NO_NEXT_REASON,
    // upgrading
    VERSION,
    OLD_VERSION,
    NEW_VERSION,
    META_DATA_VERSION,
    FORMAT_VERSION,
    LOCAL_VERSION,
    STORED_VERSION,
    NEW_FORMAT_VERSION,
    CACHED_VERSION,
    // tuple range
    LOW_BYTES,
    HIGH_BYTES,
    RANGE_BYTES,
    RANGE_START,
    RANGE_END,
    // meta-data evolution
    FIELD_NAME,
    OLD_FIELD_NAME,
    NEW_FIELD_NAME,
    OLD_FIELD_TYPE,
    NEW_FIELD_TYPE,
    OLD_INDEX_NAME,
    NEW_INDEX_NAME,
    OLD_INDEX_TYPE,
    NEW_INDEX_TYPE,
    OLD_KEY_EXPRESSION,
    NEW_KEY_EXPRESSION,
    RECORD_TYPE,
    OLD_RECORD_TYPE,
    NEW_RECORD_TYPE,
    INDEX_OPTION,
    OLD_OPTION,
    NEW_OPTION,
    INDEX_TYPE,
    // resolver
    RESOLVER,
    RESOLVER_KEY,
    RESOLVER_PATH,
    RESOLVER_METADATA,
    RESOLVER_REVERSE_VALUE,
    RESOLVER_VALUE,
    VALIDATION_RESULT,
    CACHED_KEY,
    CACHED_STATE,
    READ_STATE,
    SHARED_READ_VERSION,
    MUTATION("state_mutation"),

    // query plan
    PLAN,
    PLAN_HASH,
    QUERY,

    // error
    ERROR,
    ERROR_CODE,

    // record count limits for reading/indexing
    LIMIT,
    OLD_LIMIT,
    RECORD_COUNT,
    RECORDS_SIZE_ESTIMATE,
    REBUILD_RECORD_COUNTS,
    SCANNED_SO_FAR,
    MAX_LIMIT,
    NEXT_CONTINUATION,
    SUCCESSFUL_TRANSACTIONS_COUNT,
    FAILED_TRANSACTIONS_COUNT,
    FAILED_TRANSACTIONS_COUNT_IN_RUNNER,
    TOTAL_RECORDS_SCANNED,
    TOTAL_RECORDS_SCANNED_DURING_FAILURES,

    // time limits milliseconds
    TIME_LIMIT_MILLIS("time_limit_milliseconds"),
    TIME_STARTED_MILLIS("time_started_milliseconds"),
    TIME_ENDED_MILLIS("time_ended_milliseconds"),
    TIME_TO_WAIT_MILLIS("time_to_wait_milliseconds"),

    // Log the name of the tokenizer used
    TOKENIZER_NAME,

    SYNONYM_NAME,

    //for logging asyncToSync timeout limits
    TIME_LIMIT,
    TIME_UNIT,

    // ranked set
    HASH_FUNCTION,
    HASH,

    // Lucene
    PARTITION_ID,
    PARTITIONING_KEY,

    // Record context properties
    PROPERTY_NAME,
    PROPERTY_TYPE;

    private final String logKey;

    LogMessageKeys() {
        this.logKey = name().toLowerCase(Locale.ROOT);
    }

    LogMessageKeys(@Nonnull String key) {
        this.logKey = key;
    }

    @Override
    public String toString() {
        return logKey;
    }
}
