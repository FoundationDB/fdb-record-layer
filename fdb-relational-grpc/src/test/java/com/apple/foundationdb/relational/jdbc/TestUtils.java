/*
 * TestUtils.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;

import java.util.Arrays;
import java.util.List;

public class TestUtils {
    public static RelationalResultSet resultSet(String typeName, List<Integer> columnTypes, Continuation continuation, MockResultSetRow... rows) {
        return new MockResultSet(
                new MockResultSetMetadata(typeName, columnTypes),
                Arrays.stream(rows).iterator(),
                continuation);
    }

    public static MockResultSetRow row(Object... columns) {
        return new MockResultSetRow(List.of(columns));
    }
}
