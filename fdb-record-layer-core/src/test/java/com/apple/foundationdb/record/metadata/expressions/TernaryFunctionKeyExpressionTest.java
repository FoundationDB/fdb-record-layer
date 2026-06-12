/*
 * TernaryFunctionKeyExpressionTest.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests of the {@link TernaryFunctionKeyExpression}.
 */
class TernaryFunctionKeyExpressionTest {
    @Nonnull
    private static FunctionKeyExpression create(@Nonnull KeyExpression condition, @Nonnull KeyExpression trueExpression, @Nonnull KeyExpression falseExpression) {
        FunctionKeyExpression keyExpression = Key.Expressions.function(TernaryFunctionKeyExpression.FUNCTION_NAME,
                Key.Expressions.list(condition, trueExpression, falseExpression));
        assertThat(keyExpression)
                .isInstanceOf(TernaryFunctionKeyExpression.class);
        assertThat(keyExpression.getColumnSize())
                .isEqualTo(trueExpression.getColumnSize())
                .isEqualTo(falseExpression.getColumnSize());
        assertThat(keyExpression.createsDuplicates())
                .isEqualTo(condition.createsDuplicates() || trueExpression.createsDuplicates() || falseExpression.createsDuplicates());
        return keyExpression;
    }

    private static void evaluateTrueAndFalse(@Nonnull KeyExpression trueExpr, @Nonnull KeyExpression falseExpr, @Nullable FDBRecord<?> record) {
        KeyExpression trueTernary = create(Key.Expressions.value(true), trueExpr, falseExpr);
        assertThat(trueTernary.evaluate(record))
                .containsExactlyInAnyOrderElementsOf(trueExpr.evaluate(record));
        KeyExpression falseTernary = create(Key.Expressions.value(false), trueExpr, falseExpr);
        assertThat(falseTernary.evaluate(record))
                .containsExactlyInAnyOrderElementsOf(falseExpr.evaluate(record));
    }

    @Test
    void basicOnLiteralsTest() {
        KeyExpression trueExpr = create(Key.Expressions.value(true), Key.Expressions.value(3L), Key.Expressions.value(4L));
        assertThat(trueExpr.evaluateSingleton(null))
                .isEqualTo(Key.Evaluated.scalar(3L));

        KeyExpression falseExpr = create(Key.Expressions.value(false), Key.Expressions.value(3L), Key.Expressions.value(4L));
        assertThat(falseExpr.evaluateSingleton(null))
                .isEqualTo(Key.Evaluated.scalar(4L));

        evaluateTrueAndFalse(Key.Expressions.value("foo"), Key.Expressions.value("bar"), null);
    }

    @Test
    void twoColumnResult() {
        final KeyExpression trueExpr = Key.Expressions.concat(Key.Expressions.value("hello"), Key.Expressions.value(1L));
        final KeyExpression falseExpr = Key.Expressions.concat(Key.Expressions.value("goodbye"), Key.Expressions.value(2L));

        assertThat(create(Key.Expressions.value(true), trueExpr, falseExpr).evaluateSingleton(null))
                .isEqualTo(Key.Evaluated.concatenate("hello", 1L));

        assertThat(create(Key.Expressions.value(false), trueExpr, falseExpr).evaluateSingleton(null))
                .isEqualTo(Key.Evaluated.concatenate("goodbye", 2L));

        evaluateTrueAndFalse(trueExpr, falseExpr, null);
    }

    @Test
    void mixedColumnSize() {
        assertThatCode(() -> create(Key.Expressions.field("blah"), Key.Expressions.field("one"), Key.Expressions.concatenateFields("two", "three")))
                .isInstanceOf(MetaDataException.class)
                .hasMessageContaining("left and right arguments of ternary expression must have the same result column sizes");
    }
}
