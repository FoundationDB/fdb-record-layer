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
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.metadata.KeyExpressionTest.evaluate;
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

    @Test
    public void ignoreCase() throws Exception {
        final KeyExpression expression = function(collateFunctionName, STR_FIELD);
        List<Key.Evaluated> eval1 = evaluate(expression, buildMessage("foo"));
        List<Key.Evaluated> eval2 = evaluate(expression, buildMessage("FOO"));
        assertEquals(eval1, eval2);
    }

    @Test
    public void ignoreCaseAndAccents() throws Exception {
        final KeyExpression expression = function(collateFunctionName, concat(STR_FIELD, value("en_US")));
        List<Key.Evaluated> eval1a = evaluate(expression, buildMessage("fOoBaR"));
        List<Key.Evaluated> eval2a = evaluate(expression, buildMessage("FoObAr"));
        assertEquals(eval1a, eval2a);
        List<Key.Evaluated> eval1b = evaluate(expression, buildMessage("cliche"));
        List<Key.Evaluated> eval2b = evaluate(expression, buildMessage("cliché"));
        assertEquals(eval1b, eval2b);
    }

    @Test
    public void ignoreCaseOnly() throws Exception {
        final KeyExpression expression = function(collateFunctionName, concat(STR_FIELD, value("en_US"), value(1)));
        List<Key.Evaluated> eval1a = evaluate(expression, buildMessage("fOoBaR"));
        List<Key.Evaluated> eval2a = evaluate(expression, buildMessage("FoObAr"));
        assertEquals(eval1a, eval2a);
        List<Key.Evaluated> eval1b = evaluate(expression, buildMessage("cliche"));
        List<Key.Evaluated> eval2b = evaluate(expression, buildMessage("cliché"));
        assertNotEquals(eval1b, eval2b);
    }

}
