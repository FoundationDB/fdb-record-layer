/*
 * KeyBuilderTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.relational.utils.RelationalAssertions.assertThat;
import static com.apple.foundationdb.relational.utils.RelationalAssertions.assertThrows;

class KeyBuilderTest {
    @Test
    void testEmptyKeyExpression() throws RelationalException {
        KeyBuilder keyBuilder = new KeyBuilder(null, EmptyKeyExpression.EMPTY, "empty");
        Row key = keyBuilder.buildKey(Map.of(), true);
        assertThat(key.getNumFields()).isEqualTo(0);
    }

    @Test
    void testFailOnIncompleteKey() {
        KeyBuilder keyBuilder = new KeyBuilder(null, concat(field("A"), field("B")), "test");
        assertThrows(() -> keyBuilder.buildKey(Map.of(), true))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER);
        assertThrows(() -> keyBuilder.buildKey(Map.of("A", 5), true))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void testFailOnMissingKeyAtPosition() {
        KeyBuilder keyBuilder = new KeyBuilder(null, concat(field("A"), field("B")), "test");
        assertThrows(() -> keyBuilder.buildKey(Map.of("B", 5), false))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void testPartialKey() throws RelationalException {
        KeyBuilder keyBuilder = new KeyBuilder(null, concat(field("A"), field("B")), "test");
        Row key = keyBuilder.buildKey(Map.of("A", 5), false);
        assertThat(key.getNumFields()).isEqualTo(1);
        assertThat(key.getLong(0)).isEqualTo(5L);
    }

    @Test
    void testIncompleteKeyAllNulls() throws RelationalException {
        KeyBuilder keyBuilder = new KeyBuilder(null, concat(field("A"), field("B")), "test");
        Row key = keyBuilder.buildKey(Map.of(), false);
        assertThat(key.getNumFields()).isEqualTo(0);
    }
}
