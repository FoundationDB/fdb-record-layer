/*
 * ParserTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Locale;

public class ParserTests {

    @ParameterizedTest
    @ValueSource(strings = {"__foo", "2foo", "#foo", ".foo", "__"})
    public void invalidIdentifierTest(String id) {
        final var query = String.format(Locale.ROOT, "SELECT * from %s", id);
        RelationalAssertions.assertThrows(() -> QueryParser.parse(query))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }
}
