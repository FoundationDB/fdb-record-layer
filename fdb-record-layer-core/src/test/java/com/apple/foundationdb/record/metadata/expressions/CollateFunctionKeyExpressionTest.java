/*
 * CollateFunctionKeyExpressionTest.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.metadata.KeyExpressionTest.evaluate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests for {@link CollateFunctionKeyExpression}.
 */
public abstract class CollateFunctionKeyExpressionTest {

    @Nonnull
    protected final String collateFunctionName;

    protected CollateFunctionKeyExpressionTest(@Nonnull String collateFunctionName) {
        this.collateFunctionName = collateFunctionName;
    }

    /**
     * Test with JRE collators.
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow JRE here.
    public static class CollateFunctionKeyExpressionJRETest extends CollateFunctionKeyExpressionTest {
        public CollateFunctionKeyExpressionJRETest() {
            super(CollateFunctionKeyExpressionFactoryJRE.FUNCTION_NAME);
        }
    }

    protected static final KeyExpression STR_FIELD = field("str_value_indexed");

    protected Message buildMessage(String str) {
        return TestRecords1Proto.MySimpleRecord.newBuilder()
                .setStrValueIndexed(str)
                .build();
    }

    private void assertIgnoresCase(@Nonnull KeyExpression expression) {
        List<Key.Evaluated> eval1a = evaluate(expression, buildMessage("foo"));
        List<Key.Evaluated> eval2a = evaluate(expression, buildMessage("FOO"));
        assertEquals(eval1a, eval2a);

        List<Key.Evaluated> eval1b = evaluate(expression, buildMessage("fOoBaR"));
        List<Key.Evaluated> eval2b = evaluate(expression, buildMessage("FoObAr"));
        assertEquals(eval1b, eval2b);
    }

    @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
    private void assertAccents(@Nonnull KeyExpression expression, boolean ignored) {
        List<Key.Evaluated> eval1a = evaluate(expression, buildMessage("cliche"));
        List<Key.Evaluated> eval2a = evaluate(expression, buildMessage("cliché"));
        if (ignored) {
            assertEquals(eval1a, eval2a);
        } else {
            assertNotEquals(eval1a, eval2a);
        }

        // These two forms are different normalized forms for the same accented form.
        // The former uses the combined composition (i.e., one character for é), and
        // the latter uses the decomposed form (i.e., one character for e and a combining
        // character for the acute accent).
        // They should normalize to the same thing after the collation function
        List<Key.Evaluated> eval1b = evaluate(expression, buildMessage("clich\u00e9"));
        List<Key.Evaluated> eval2b = evaluate(expression, buildMessage("cliche\u0301"));
        assertEquals(eval1b, eval2b);

        assertEquals(eval1b, eval2a);
        if (ignored) {
            assertEquals(eval1a, eval1b);
        } else {
            assertNotEquals(eval1a, eval1b);
        }
    }

    private void assertExpectedOrder(@Nonnull KeyExpression expression, @Nonnull String... preCollationStrings) {
        String previous = null;
        Key.Evaluated previousEval = null;
        for (String value : preCollationStrings) {
            Key.Evaluated eval = Iterables.getOnlyElement(evaluate(expression, buildMessage(value)));
            if (previousEval != null) {
                assertThat("Key for \"" + value + "\" should be after key for \"" + previous + "\"",
                        eval.toTuple(), greaterThanOrEqualTo(previousEval.toTuple()));
            }
            previousEval = eval;
            previous = value;
        }
    }

    @Test
    void ignoreCase() {
        final KeyExpression expression = function(collateFunctionName, STR_FIELD);
        assertIgnoresCase(expression);
    }

    @Test
    void ignoreCaseAndAccents() {
        final KeyExpression expression = function(collateFunctionName, concat(STR_FIELD, value("en_US")));
        assertIgnoresCase(expression);
        assertAccents(expression, true);
    }

    @Test
    void ignoreCaseOnly() {
        final KeyExpression expression = function(collateFunctionName, concat(STR_FIELD, value("en_US"), value(1)));
        assertIgnoresCase(expression);
        assertAccents(expression, false);
    }

    @Test
    void defaultCollation() {
        final KeyExpression expression = function(collateFunctionName, STR_FIELD);
        assertIgnoresCase(expression);
        assertAccents(expression, true);
        assertExpectedOrder(expression,
                "0", "08", "7",
                "A", "a", "aA", "Ba", "bc", "cf", "ch", "cz", "d", "e f", "ef",
                "Γ", "Д", "ف", "你好");
    }
}
