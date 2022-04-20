/*
 * OperationOption.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

/**
 * Options related to a specific table operation.
 *
 * @param <T> the type of the value held in the operation.
 */
public class OperationOption<T> {
    public static final String INDEX_HINT_NAME = "use.index";

    public static final String CONTINUATION_NAME = "continuation";

    public static final String SCHEMA_EXISTENCE_CHECK = "schema.existenceCheck";

    public enum SchemaExistenceCheck {
        /**
         * No special action. This should be used with care, since if the schema already has records,
         * there is no guarantee that they were written at the current versions (meta-data and format).
         * It is really only appropriate in development when switching from uncheckedOpen or build to a checked open.
         */
        NONE,
        /**
         * Throw if the schema does not have an info header but does have have at least one record.
         * This differs from ERROR_IF_NO_INFO_AND_NOT_EMPTY in that there is data stored in the schema other than
         * just the records and the indexes, including meta-data about which indexes have been built.
         * A schema that is missing a header but has this other data is in a corrupt state, but as there are no records,
         * it can be recovered when creating the schema in a straightforward way.
         */
        ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES,
        /**
         * Throw if the record store does not have an info header but is not empty.
         * Unlike with ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES, this existence check will throw an error even
         * if there are no records in the store, only data stored internally by the Relational.
         */
        ERROR_IF_NO_INFO_AND_NOT_EMPTY,
        /**
         * Throw if the schema already exists.
         */
        ERROR_IF_EXISTS,
        /**
         * Throw if the schema does not already exist.
         */
        ERROR_IF_NOT_EXISTS
    }

    final String optionName;
    final T value;

    public OperationOption(String optionName, T value) {
        this.optionName = optionName;
        this.value = value;
    }

    String getOptionName() {
        return optionName;
    }

    T getValue() {
        return value;
    }

    public static OperationOption<String> index(String indexName) {
        return new OperationOption<>(INDEX_HINT_NAME, indexName);
    }

    public static OperationOption<Continuation> continuation(Continuation continuation) {
        return new OperationOption<>(CONTINUATION_NAME, continuation);
    }

}
