/*
 * IndexTarget.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;

/**
 * Roots a definition's value expression at wherever the {@code IndexedMessage} lives for the current
 * scenario, and supplies the grouping prefix. A definition writes its value expression <em>relative
 * to an {@code IndexedMessage}</em> (e.g. {@code field("int_value")}) and the framework's target
 * roots it at the right place, so the same {@link IndexDefinition#buildIndex} serves the normal,
 * grouped, joined, and unnested scenarios:
 * <ul>
 *     <li>normal: {@code field("indexed").nest(within)}</li>
 *     <li>joined: {@code field("simple").nest(field("indexed").nest(within))}</li>
 *     <li>unnested: {@code field("entry").nest(within)}</li>
 * </ul>
 * (All of these are {@code field(...).nest(...)}, so they return a {@link NestingKeyExpression},
 * which offers {@code groupBy}/{@code ungrouped} for definitions that need them.)
 */
public interface IndexTarget {
    /**
     * Root a value expression, written relative to the {@code IndexedMessage}, at its actual location.
     *
     * @param withinIndexedMessage the value expression relative to the indexed message
     * @return the rooted expression
     */
    NestingKeyExpression indexed(KeyExpression withinIndexedMessage);

    /**
     * Convenience for a single scalar field of the indexed message.
     *
     * @param fieldName the field name within the indexed message
     * @return the rooted field expression
     */
    default NestingKeyExpression indexedField(final String fieldName) {
        return indexed(Key.Expressions.field(fieldName));
    }

    /**
     * The grouping prefix to prepend to the index: {@code field("group")} for the grouped
     * ({@code deleteRecordsWhere}) scenario, or an empty expression for ungrouped and synthetic
     * scenarios.
     *
     * @return the grouping prefix expression
     */
    KeyExpression groupingPrefix();
}
