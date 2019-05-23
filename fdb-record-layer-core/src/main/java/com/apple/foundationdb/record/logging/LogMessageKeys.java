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

/**
 * Common {@link KeyValueLogMessage} keys logged by the Record Layer core.
 * In general, we try to consolidate all of the keys here, so that it's easy to check for collisions, ensure consistency, etc.
 */
@API(API.Status.UNSTABLE)
public enum LogMessageKeys {
    // general keys
    TITLE("ttl"),
    CLUSTER("cluster"),
    SUBSPACE("subspace"),
    SUBSPACE_KEY("subspace_key"),
    INDEX_SUBSPACE("indexSubspace"),
    CALLING_CLASS("calling_class"),
    CALLING_METHOD("calling_method"),
    CALLING_LINE("calling_line"),
    FUTURE_COMPLETED("future_completed"),
    SCAN_PROPERTIES("scan_properties"),
    READ_VERSION("read_version"),
    OLD("old"),
    NEW("new"),
    MESSAGE("message"),
    CODE("code"),
    DESCRIPTION("description"),
    FAIL("fail"),
    SEED("seed"),
    ITERATION("iteration"),
    UNKNOWN_FIELDS("unknownFields"),
    TRIES("tries"),
    MAX_ATTEMPTS("max_attempts"),
    DELAY("delay"),
    // record splitting/unsplitting
    KEY("key"),
    KEY_TUPLE("key_tuple"),
    EXPECTED_INDEX("expected_index"),
    FOUND_INDEX("found_index"),
    SPLIT_NEXT_INDEX("next_index"),
    SPLIT_REVERSE("reverse"),
    // protobuf parsing
    RAW_BYTES("raw_bytes"),
    // key space API keys
    PARENT_DIR("parent_dir"),
    DIR_NAME("dir_name"),
    DIR_TYPE("dir_type"),
    DIR_VALUE("dir_value"),
    PROVIDED_KEY("provided_key"),
    PROVIDED_VALUE("provided_value"),
    LIST_FROM("list_from"),
    SUBDIR_NAME("subdir_name"),
    RANGE("range"),
    RANGE_VALUE_TYPE("range_value_type"),
    EXPECTED_VALUE_TYPE("expected_value_type"),
    ENDPOINT_TYPE("endpoint_type"),
    // key expressions
    EXPECTED_COLUMN_SIZE("expected_column_size"),
    ACTUAL_COLUMN_SIZE("actual_column_size"),
    KEY_EXPRESSION("key_expression"),
    KEY_EXPRESSION_C("keyExpression"),
    KEY_EVALUATED("key_evaluated"),
    // manipulating subkeys of key expressions
    REQUESTED_START("requested_start"),
    REQUESTED_END("requested_end"),
    COLUMN_SIZE("column_size"),
    // query components
    QUERY("query"),
    FILTER("filter"),
    PARENT_FILTER("parent_filter"),
    CHILD_FILTER("child_filter"),
    OTHER_FILTER("other_filter"),
    // Boolean normalization
    DNF_SIZE("dnf_size"),
    DNF_SIZE_LIMIT("dnf_size_limit"),
    // index-related keys
    INDEX("index"),
    INDEX_NAME("index_name"),
    INDEX_NAME_C("indexName"),
    INDEX_VERSION_C("indexVersion"),
    VALUE_KEY("value_key"),
    PRIMARY_KEY("primary_key"),
    PRIMARY_KEY_C("primaryKey"),
    VALUE("value"),
    INDEX_OPERATION("operation"),
    INITIAL_PREFIX("initial_prefix"),
    SECOND_PREFIX("second_prefix"),
    INDEX_VERSION("index_version"),
    START_TUPLE("startTuple"),
    END_TUPLE("endTuple"),
    REAL_END("realEnd"),
    RECORDS_SCANNED("recordsScanned"),
    ORIGINAL_RANGE("originalRange"),
    SPLIT_RANGES("splitRanges"),
    REASON("reason"),
    LATEST_ENTRY_TIMESTAMP("latestEntryTimestamp"),
    EARLIEST_ADDED_START_TIMESTAMP("earliestAddedStartTimestamp"),
    REMOVE("remove"),
    TEXT_SIZE("textSize"),
    UNIQUE_TOKENS("uniqueTokens"),
    AVG_TOKEN_SIZE("avgTokenSize"),
    MAX_TOKEN_SIZE("maxTokenSize"),
    AVG_POSITIONS("avgPositions"),
    MAX_POSITIONS("maxPositions"),
    TEXT_KEY_SIZE("textKeySize"),
    TEXT_VALUE_SIZE("textValueSize"),
    TEXT_INDEX_SIZE_AMORTIZED("textIndexSizeAmortized"),
    WROTE_INDEX("wroteIndex"),
    NEW_STORE("newStore"),
    RECORDS("records"),
    RECORDS_WHILE_BUILDING("recordsWhileBuilding"),
    AGENTS("agents"),
    AGENT("agent"),
    OVERLAP("overlap"),
    SPLIT_LONG_RECORDS("splitLongRecords"),
    RECORDS_PER_SECOND("recordsPerSecond"),
    BEGIN("begin"),
    END("end"),
    DOCUMENT("document"),
    BATCH_SIZE("batch_size"),
    SCAN_MILLIS("scan_millis"),
    MANUAL_RESULT_COUNT("manual_result_count"),
    QUERY_RESULT_COUNT("query_result_count"),
    ONLY_MANUAL_COUNT("only_manual_count"),
    ONLY_QUERY_COUNT("only_query_count"),
    RESULT_COUNT("result_count"),
    TOTAL_SCAN_MILLIS("total_scan_millis"),
    TOTAL_QUERY_MILLIS("total_query_millis"),
    TOTAL_RESULT_COUNT("total_result_count"),

    // comparisons
    COMPARISON_VALUE("comparison_value"),
    // functional index keys
    FUNCTION("function"),
    INDEX_KEY("index_key"),
    KEY_SPACE_PATH("key_space_path"),
    INDEX_VALUE("index_value"),
    ARGUMENT_INDEX("argument_index"),
    EVALUATED_SIZE("evaluated_size"),
    EVALUATED_INDEX("evaluated_index"),
    EXPECTED_TYPE("expected_type"),
    ACTUAL_TYPE("actual_type"),
    // cursors
    CHILD_COUNT("child_count"),
    EXPECTED_CHILD_COUNT("expected_child_count"),
    READ_CHILD_COUNT("read_child_count"),
    TIME_STARTED("time_started"),
    TIME_ENDED("time_ended"),
    DURATION_MILLIS("duration_millis"),
    CURSOR_ELAPSED_MILLIS("cursorElapsedMillis"),
    CURSOR_TIME_LIMIT_MILLIS("cursorTimeLimitMillis"),
    // upgrading
    VERSION("version"),
    OLD_VERSION("old_version"),
    NEW_VERSION("new_version"),
    META_DATA_VERSION("metaDataVersion"),
    FORMAT_VERSION("format_version"),
    LOCAL_VERSION("localVersion"),
    STORED_VERSION("storedVersion"),
    NEW_FORMAT_VERSION("newFormatVersion"),
    CACHED_VERSION("cachedVersion"),
    // tuple range
    LOW_BYTES("lowBytes"),
    HIGH_BYTES("highBytes"),
    RANGE_BYTES("rangeBytes"),
    RANGE_START("rangeStart"),
    RANGE_END("rangeEnd"),
    // meta-data evolution
    FIELD_NAME("field_name"),
    OLD_FIELD_NAME("old_field_name"),
    NEW_FIELD_NAME("new_field_name"),
    OLD_FIELD_TYPE("old_field_type"),
    NEW_FIELD_TYPE("new_field_type"),
    OLD_INDEX_NAME("old_index_name"),
    NEW_INDEX_NAME("new_index_name"),
    OLD_INDEX_TYPE("old_index_type"),
    NEW_INDEX_TYPE("new_index_type"),
    OLD_KEY_EXPRESSION("old_key_expression"),
    NEW_KEY_EXPRESSION("new_key_expression"),
    RECORD_TYPE("record_type"),
    RECORD_TYPE_C("recordType"),
    RECORD_TYPES("recordTypes"),
    OLD_RECORD_TYPE("old_record_type"),
    NEW_RECORD_TYPE("new_record_type"),
    INDEX_OPTION("index_option"),
    OLD_OPTION("old_option"),
    NEW_OPTION("new_option"),
    INDEX_TYPE("indexType"),
    // resolver
    RESOLVER_KEY("resolverKey"),
    RESOLVER_PATH("resolverPath"),
    CACHED_STATE("cachedState"),
    READ_STATE("readState"),

    // query plan
    PLAN("plan"),
    PLAN_HASH("planHash"),

    // error
    ERROR("error"),
    ERROR_CODE("errorCode"),

    // record count limits for reading/indexing
    LIMIT("limit"),
    RECORD_COUNT("recordCount"),
    REBUILD_RECORD_COUNTS("rebuildRecordCounts"),
    SCANNED_SO_FAR("scannedSoFar"),
    NEXT_CONTINUATION("nextContinuation"),

    // Log the name of the tokenizer used
    TOKENIZER_NAME("tokenizer_name");

    private final String logKey;

    LogMessageKeys(@Nonnull String key) {
        this.logKey = key;
    }

    @Override
    public String toString() {
        return logKey;
    }
}
