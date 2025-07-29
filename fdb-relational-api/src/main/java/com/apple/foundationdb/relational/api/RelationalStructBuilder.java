/*
 * RelationalStructBuilder.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.UUID;

/**
 * For implementation by {@link RelationalStruct} <a href="https://refactoring.guru/design-patterns/builder">Builder</a>.
 * There is no metadata available while {@link RelationalStruct} instance when under construction.
 * The order in which columns are set is preserved and is the order in which fields are returned when accessed by
 * field/column index.
 */
// TODO: Add positional add... we only have by column name currently. See if demand first.
public interface RelationalStructBuilder {
    /**
     * Build a {@link RelationalStruct}.
     * @return A 'built' RelationalStruct instance.
     */
    RelationalStruct build();

    RelationalStructBuilder addBoolean(String fieldName, boolean b) throws SQLException;

    RelationalStructBuilder addLong(String fieldName, long l) throws SQLException;

    RelationalStructBuilder addFloat(String fieldName, float f) throws SQLException;

    RelationalStructBuilder addDouble(String fieldName, double d) throws SQLException;

    RelationalStructBuilder addBytes(String fieldName, byte[] bytes) throws SQLException;

    RelationalStructBuilder addString(String fieldName, @Nullable String s) throws SQLException;

    RelationalStructBuilder addUuid(String fieldName, @Nullable UUID uuid) throws SQLException;

    RelationalStructBuilder addObject(String fieldName, @Nullable Object obj) throws SQLException;

    RelationalStructBuilder addStruct(String fieldName, @Nonnull RelationalStruct struct) throws SQLException;

    RelationalStructBuilder addArray(String fieldName, @Nonnull RelationalArray array) throws SQLException;

    RelationalStructBuilder addInt(String fieldName, int i) throws SQLException;
}
