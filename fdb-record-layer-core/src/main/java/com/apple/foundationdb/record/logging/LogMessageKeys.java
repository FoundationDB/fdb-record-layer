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
    INDEX_SUBSPACE("index_subspace"),
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
    UNKNOWN_FIELDS("unknown_fields"),
    CURR_ATTEMPT("curr_attempt"),
    MAX_ATTEMPTS("max_attempts"),
    DELAY("delay"),
    COMMIT_NAME("commit_name"),
    TRANSACTION_ID("transaction_id"),
    TRANSACTION_NAME("transaction_name"),
    AGE_SECONDS("age_seconds"),
    // record splitting/unsplitting
    KEY("key"),
    KEY_TUPLE("key_tuple"),
    EXPECTED_INDEX("expected_index"),
    FOUND_INDEX("found_index"),
    KNOWN_LAST_KEY("known_last_key"),
    SPLIT_NEXT_INDEX("next_index"),
    SPLIT_REVERSE("reverse"),
    READ_LAST_KEY_MICROS("read_last_key_micros"),
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
    SUBDIRECTORY("subdirectory"),
    RANGE("range"),
    RANGE_VALUE_TYPE("range_value_type"),
    EXPECTED_VALUE_TYPE("expected_value_type"),
    ENDPOINT_TYPE("endpoint_type"),
    // stored size info
    KEY_COUNT("key_count"),
    KEY_SIZE("key_size"),
    VALUE_SIZE("value_size"),
    IS_SPLIT("is_split"),
    IS_VERSIONED_INLINE("is_versioned_inline"),
    // key expressions
    EXPECTED_COLUMN_SIZE("expected_column_size"),
    ACTUAL_COLUMN_SIZE("actual_column_size"),
    KEY_EXPRESSION("key_expression"),
    KEY_EVALUATED("key_evaluated"),
    // manipulating subkeys of key expressions
    REQUESTED_START("requested_start"),
    REQUESTED_END("requested_end"),
    COLUMN_SIZE("column_size"),
    // query components
    FILTER("filter"),
    PARENT_FILTER("parent_filter"),
    CHILD_FILTER("child_filter"),
    OTHER_FILTER("other_filter"),
    // Boolean normalization
    DNF_SIZE("dnf_size"),
    DNF_SIZE_LIMIT("dnf_size_limit"),
    // index-related keys
    INDEX_NAME("index_name"),
    INDEX_STATE("index_state"),
    VALUE_KEY("value_key"),
    PRIMARY_KEY("primary_key"),
    VALUE("value"),
    INDEX_OPERATION("operation"),
    INITIAL_PREFIX("initial_prefix"),
    SECOND_PREFIX("second_prefix"),
    INDEX_VERSION("index_version"),
    START_TUPLE("start_tuple"),
    END_TUPLE("end_tuple"),
    REAL_END("real_end"),
    RECORDS_SCANNED("records_scanned"),
    ORIGINAL_RANGE("original_range"),
    SPLIT_RANGES("split_ranges"),
    REASON("reason"),
    LATEST_ENTRY_TIMESTAMP("latest_entry_timestamp"),
    EARLIEST_ADDED_START_TIMESTAMP("earliest_added_start_timestamp"),
    REMOVE("remove"),
    TEXT_SIZE("text_size"),
    UNIQUE_TOKENS("unique_tokens"),
    AVG_TOKEN_SIZE("avg_token_size"),
    MAX_TOKEN_SIZE("max_token_size"),
    AVG_POSITIONS("avg_positions"),
    MAX_POSITIONS("max_positions"),
    TEXT_KEY_SIZE("text_key_size"),
    TEXT_VALUE_SIZE("text_value_size"),
    TEXT_INDEX_SIZE_AMORTIZED("text_index_size_amortized"),
    WROTE_INDEX("wrote_index"),
    NEW_STORE("new_store"),
    RECORDS_WHILE_BUILDING("records_while_building"),
    DOCUMENT("document"),
    SESSION_ID("session_id"),
    INDEXER_SESSION_ID("indexer_session_id"),
    INDEXER_ID("indexer_id"),
    INDEX_STATE_PRECONDITION("index_state_precondition"),
    INITIAL_INDEX_STATE("initial_index_state"),
    SHOULD_BUILD_INDEX("do_build_index"),
    SHOULD_CLEAR_EXISTING_DATA("clear_existing_data"),
    SHOULD_MARK_READABLE("should_mark_readable"),
    RESULT("result"),
    INDEXER_CURR_RETRY("indexer_curr_retry"),
    INDEXER_MAX_RETRIES("indexer_max_retries"),
    DIRECTOY("directory"),
    SOURCE_INDEX("source_index"),
    SOURCE_FILE("source_file"),
    CONTINUED_BUILD("continued_build"),
    INDEXING_METHOD("indexing_method"),
    ALLOW_REPAIR("allow_repair"),
    INDEXING_POLICY("indexing_policy"),
    INDEXING_POLICY_DESIRED_ACTION("indexing_policy_desired_action"),

    // comparisons
    COMPARISON_VALUE("comparison_value"),
    EXPECTED("expected"),
    ACTUAL("actual"),
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
    CURSOR_ELAPSED_MILLIS("cursor_elapsed_millis"),
    CURSOR_TIME_LIMIT_MILLIS("cursor_time_limit_millis"),
    NO_NEXT_REASON("no_next_reason"),
    // upgrading
    VERSION("version"),
    OLD_VERSION("old_version"),
    NEW_VERSION("new_version"),
    META_DATA_VERSION("meta_data_version"),
    FORMAT_VERSION("format_version"),
    LOCAL_VERSION("local_version"),
    STORED_VERSION("stored_version"),
    NEW_FORMAT_VERSION("new_format_version"),
    CACHED_VERSION("cached_version"),
    // tuple range
    LOW_BYTES("low_bytes"),
    HIGH_BYTES("high_bytes"),
    RANGE_BYTES("range_bytes"),
    RANGE_START("range_start"),
    RANGE_END("range_end"),
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
    OLD_RECORD_TYPE("old_record_type"),
    NEW_RECORD_TYPE("new_record_type"),
    INDEX_OPTION("index_option"),
    OLD_OPTION("old_option"),
    NEW_OPTION("new_option"),
    INDEX_TYPE("index_type"),
    // resolver
    RESOLVER("resolver"),
    RESOLVER_KEY("resolver_key"),
    RESOLVER_PATH("resolver_path"),
    RESOLVER_METADATA("resolver_metadata"),
    RESOLVER_REVERSE_VALUE("resolver_reverse_value"),
    RESOLVER_VALUE("resolver_value"),
    VALIDATION_RESULT("validation_result"),
    CACHED_KEY("cached_key"),
    CACHED_STATE("cached_state"),
    READ_STATE("read_state"),
    SHARED_READ_VERSION("shared_read_version"),
    MUTATION("state_mutation"),

    // query plan
    PLAN("plan"),

    // error
    ERROR("error"),
    ERROR_CODE("error_code"),

    // record count limits for reading/indexing
    LIMIT("limit"),
    RECORD_COUNT("record_count"),
    RECORDS_SIZE_ESTIMATE("records_size_estimate"),
    REBUILD_RECORD_COUNTS("rebuild_record_counts"),
    SCANNED_SO_FAR("scanned_so_far"),
    MAX_LIMIT("max_limit"),
    NEXT_CONTINUATION("next_continuation"),

    // Log the name of the tokenizer used
    TOKENIZER_NAME("tokenizer_name"),

    //for logging asyncToSync timeout limits
    TIME_LIMIT("time_limit"),
    TIME_UNIT("time_unit"),

    // ranked set
    HASH_FUNCTION("hash_function"),
    HASH("hash");

    private final String logKey;

    LogMessageKeys(@Nonnull String key) {
        this.logKey = key;
    }

    @Override
    public String toString() {
        return logKey;
    }
}
