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

import com.apple.foundationdb.API;

import javax.annotation.Nonnull;

/**
 * Common {@link KeyValueLogMessage} keys logged by the Record Layer core.
 * In general, we try to consolidate all of the keys here, so that it's easy to check for collisions, ensure consistency, etc.
 */
@API(API.Status.UNSTABLE)
public enum LogMessageKeys {
    // general keys
    TITLE("ttl"),
    SUBSPACE("subspace"),
    SUBSPACE_KEY("subspace_key"),
    CALLING_CLASS("calling_class"),
    CALLING_METHOD("calling_method"),
    CALLING_LINE("calling_line"),
    FUTURE_COMPLETED("future_completed"),
    SCAN_PROPERTIES("scan_properties"),
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
    VALUE_KEY("value_key"),
    PRIMARY_KEY("primary_key"),
    VALUE("value"),
    INDEX_OPERATION("operation"),
    INITIAL_PREFIX("initial_prefix"),
    SECOND_PREFIX("second_prefix"),
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
    // upgrading
    VERSION("version"),
    OLD_VERSION("old_version"),
    NEW_VERSION("new_version"),
    META_DATA_VERSION("metaDataVersion"),
    FORMAT_VERSION("format_version"),
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
    OLD_RECORD_TYPE("old_record_type"),
    NEW_RECORD_TYPE("new_record_type"),
    INDEX_OPTION("index_option"),
    OLD_OPTION("old_option"),
    NEW_OPTION("new_option"),

    // resolver
    RESOLVER_KEY("resolverKey"),
    RESOLVER_PATH("resolverPath"),

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
