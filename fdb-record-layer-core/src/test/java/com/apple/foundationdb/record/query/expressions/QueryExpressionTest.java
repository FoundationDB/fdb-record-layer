/*
 * QueryExpressionTest.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.UnstoredRecord;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto;
import com.apple.foundationdb.record.metadata.ExpressionTestsProto.TestScalarFieldAccess;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.TestHelpers.assertThrows;
import static com.apple.foundationdb.record.query.expressions.Query.and;
import static com.apple.foundationdb.record.query.expressions.Query.field;
import static com.apple.foundationdb.record.query.expressions.Query.not;
import static com.apple.foundationdb.record.query.expressions.Query.or;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link QueryComponent} expressions.
 */
public class QueryExpressionTest {

    private Boolean evaluate(@Nonnull QueryComponent component, @Nullable Message record) {
        return evaluate(component, Bindings.EMPTY_BINDINGS, record);
    }

    private Boolean evaluate(@Nonnull QueryComponent component, @Nonnull Bindings bindings, @Nullable Message record) {
        return component.eval(null, EvaluationContext.forBindings(bindings), new UnstoredRecord<>(record));
    }

    private static final byte[] DEADC0DE = new byte[]{(byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde};
    private static final byte[] LEET = new byte[]{(byte)0x1e, (byte)0xe7};
    private static final byte[] EMPTY_BYTES = new byte[]{};

    private abstract static class TestMessageComponent implements ComponentWithNoChildren {
        @Override
        public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return 0;
        }

        @Override
        public GraphExpansion expand(@Nonnull final CorrelationIdentifier base, @Nonnull final List<String> fieldNamePrefix) {
            throw new UnsupportedOperationException();
        }
    }

    private static final QueryComponent TRUE = new TestMessageComponent() {
        @Nullable
        @Override
        public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                       @Nullable FDBRecord<M> record, @Nullable Message message) {
            return true;
        }
    };
    private static final QueryComponent FALSE = new TestMessageComponent() {
        @Nullable
        @Override
        public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                       @Nullable FDBRecord<M> record, @Nullable Message message) {
            return false;
        }
    };
    private static final QueryComponent NULL = new TestMessageComponent() {
        @Nullable
        @Override
        public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                       @Nullable FDBRecord<M> record, @Nullable Message message) {
            return null;
        }
    };

    @ParameterizedTest(name = "testOneOfThemEqualsValue [emptyMode = {0}]")
    @EnumSource(Field.OneOfThemEmptyMode.class)
    public void testOneOfThemEqualsValue(Field.OneOfThemEmptyMode emptyMode) throws Exception {
        final TestScalarFieldAccess oneRepeatedValue = TestScalarFieldAccess.newBuilder()
                .addRepeatMe("fishes")
                .build();
        final QueryComponent component = field("repeat_me").oneOfThem(emptyMode).equalsValue("fishes");
        component.validate(TestScalarFieldAccess.getDescriptor());
        final Boolean eval = evaluate(component, oneRepeatedValue);
        assertEquals(Boolean.TRUE, eval);
    }

    @ParameterizedTest(name = "testOneOfThemEqualsNoValues [emptyMode = {0}]")
    @EnumSource(Field.OneOfThemEmptyMode.class)
    public void testOneOfThemEqualsNoValues(Field.OneOfThemEmptyMode emptyMode) throws Exception {
        final TestScalarFieldAccess noRepeatedValues = TestScalarFieldAccess.newBuilder().build();
        final QueryComponent component = field("repeat_me").oneOfThem(emptyMode).equalsValue("fishes");
        component.validate(TestScalarFieldAccess.getDescriptor());
        final Boolean eval = evaluate(component, noRepeatedValues);
        assertEquals(emptyMode == Field.OneOfThemEmptyMode.EMPTY_UNKNOWN ? null : Boolean.FALSE, eval);
    }

    @Test
    public void testAnd() throws Exception {
        final TestScalarFieldAccess val = TestScalarFieldAccess.newBuilder().build();
        assertNull(evaluate(and(TRUE, NULL), val));
        // Use equals here, because assertTrue/False would throw nullPointerException if and() returns null
        assertEquals(true, evaluate(and(TRUE, TRUE), val));
        assertEquals(false, evaluate(and(TRUE, FALSE), val));
        assertEquals(false, evaluate(and(NULL, FALSE), val));
        assertNull(evaluate(and(NULL, TRUE), val));
    }

    @Test
    public void testOr() throws Exception {
        final TestScalarFieldAccess val = TestScalarFieldAccess.newBuilder().build();
        assertNull(evaluate(or(FALSE, NULL), val));
        // Use equals here, because assertTrue/False would throw nullPointerException if or() returns null
        assertEquals(true, evaluate(or(FALSE, TRUE), val));
        assertEquals(true, evaluate(or(TRUE, FALSE), val));
        assertEquals(false, evaluate(or(FALSE, FALSE), val));
        assertEquals(true, evaluate(or(NULL, TRUE), val));
    }

    @Test
    public void testNot() throws Exception {
        final TestScalarFieldAccess val = TestScalarFieldAccess.newBuilder().build();
        assertNull(evaluate(not(NULL), val));
        assertEquals(true, evaluate(not(FALSE), val));
        assertEquals(false, evaluate(not(TRUE), val));
    }

    public static final Object[][] COMPARISON_TESTS = new Object[][] {
        /*  name,       field,   val1,   comparison,             val2,  expected */
        { "abc == abc", "field", "abc", Comparisons.Type.EQUALS, "abc", Boolean.TRUE },
        { "abc == xyz", "field", "abc", Comparisons.Type.EQUALS, "xyz", Boolean.FALSE },
        { "abc != xyz", "field", "abc", Comparisons.Type.NOT_EQUALS, "xyz", Boolean.TRUE },
        { "abc != abc", "field", "abc", Comparisons.Type.NOT_EQUALS, "abc", Boolean.FALSE },
        { "abc < bcd", "field", "abc", Comparisons.Type.LESS_THAN, "bcd", Boolean.TRUE },
        { "abc < aaa", "field", "abc", Comparisons.Type.LESS_THAN, "aaa", Boolean.FALSE },
        { "abc <= bcd", "field", "abc", Comparisons.Type.LESS_THAN_OR_EQUALS, "bcd", Boolean.TRUE },
        { "abc <= aaa", "field", "abc", Comparisons.Type.LESS_THAN_OR_EQUALS, "aaa", Boolean.FALSE },
        { "abc > aaa", "field", "abc", Comparisons.Type.GREATER_THAN, "aaa", Boolean.TRUE },
        { "abc > bcd", "field", "abc", Comparisons.Type.GREATER_THAN, "bcd", Boolean.FALSE },
        { "abc >= aaa", "field", "abc", Comparisons.Type.GREATER_THAN_OR_EQUALS, "aaa", Boolean.TRUE },
        { "abc >= bcd", "field", "abc", Comparisons.Type.GREATER_THAN_OR_EQUALS, "bcd", Boolean.FALSE },
        { "abc STARTS_WITH a", "field", "abc", Comparisons.Type.STARTS_WITH, "a", Boolean.TRUE },
        { "abc STARTS_WITH bc", "field", "abc", Comparisons.Type.STARTS_WITH, "bc", Boolean.FALSE },
        { "null IS NULL", "field", null, Comparisons.Type.IS_NULL, null, Boolean.TRUE },
        { "abc IS NULL", "field", "abc", Comparisons.Type.IS_NULL, null, Boolean.FALSE },
        { "abc NOT NULL", "field", "abc", Comparisons.Type.NOT_NULL, null, Boolean.TRUE },
        { "null NOT NULL", "field", null, Comparisons.Type.NOT_NULL, null, Boolean.FALSE },
        { "\\xde\\xad\\xc0\\xde == \\xde\\xad\\xc0\\xde", "bytes_field", DEADC0DE, Comparisons.Type.EQUALS, DEADC0DE, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde == \\x1e\\xe7", "bytes_field", DEADC0DE, Comparisons.Type.EQUALS, LEET, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde == ``", "bytes_field", DEADC0DE, Comparisons.Type.EQUALS, EMPTY_BYTES, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde != \\xde\\xad\\xc0\\xde", "bytes_field", DEADC0DE, Comparisons.Type.NOT_EQUALS, DEADC0DE, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde != \\x1e\\xe7", "bytes_field", DEADC0DE, Comparisons.Type.NOT_EQUALS, LEET, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde != ``", "bytes_field", DEADC0DE, Comparisons.Type.NOT_EQUALS, EMPTY_BYTES, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde < \\x1e\\xe7", "bytes_field", DEADC0DE, Comparisons.Type.LESS_THAN, LEET, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde < ``", "bytes_field", DEADC0DE, Comparisons.Type.LESS_THAN, EMPTY_BYTES, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde < \\xde\\xad\\xc0\\xde", "bytes_field", DEADC0DE, Comparisons.Type.LESS_THAN, DEADC0DE, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde <= \\x1e\\xe7", "bytes_field", DEADC0DE, Comparisons.Type.LESS_THAN_OR_EQUALS, LEET, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde <= ``", "bytes_field", DEADC0DE, Comparisons.Type.LESS_THAN_OR_EQUALS, EMPTY_BYTES, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde <= \\xde\\xad\\xc0\\xde", "bytes_field", DEADC0DE, Comparisons.Type.LESS_THAN_OR_EQUALS, DEADC0DE, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde > \\x1e\\xe7", "bytes_field", DEADC0DE, Comparisons.Type.GREATER_THAN, LEET, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde > ``", "bytes_field", DEADC0DE, Comparisons.Type.GREATER_THAN, EMPTY_BYTES, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde > \\xde\\xad\\xc0\\xde", "bytes_field", DEADC0DE, Comparisons.Type.GREATER_THAN, DEADC0DE, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde >= \\x1e\\xe7", "bytes_field", DEADC0DE, Comparisons.Type.GREATER_THAN_OR_EQUALS, LEET, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde >= ``", "bytes_field", DEADC0DE, Comparisons.Type.GREATER_THAN_OR_EQUALS, EMPTY_BYTES, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde >= \\xde\\xad\\xc0\\xde", "bytes_field", DEADC0DE, Comparisons.Type.GREATER_THAN_OR_EQUALS, DEADC0DE, Boolean.TRUE},
        { "null IS NULL", "bytes_field", null, Comparisons.Type.IS_NULL, null, Boolean.TRUE},
        { "\\xde\\xad\\xc0\\xde IS NULL", "bytes_field", DEADC0DE, Comparisons.Type.IS_NULL, null, Boolean.FALSE},
        { "`` IS NULL", "bytes_field", EMPTY_BYTES, Comparisons.Type.IS_NULL, null, Boolean.FALSE},
        { "null NOT NULL", "bytes_field", null, Comparisons.Type.NOT_NULL, null, Boolean.FALSE},
        { "\\xde\\xad\\xc0\\xde NOT NULL", "bytes_field", DEADC0DE, Comparisons.Type.NOT_NULL, null, Boolean.TRUE},
        { "`` NOT NULL", "bytes_field", EMPTY_BYTES, Comparisons.Type.NOT_NULL, null, Boolean.TRUE},
        { "a IN {a}", "field", "a", Comparisons.Type.IN, Collections.singletonList("a"), Boolean.TRUE},
        { "a IN {a,b,c}", "field", "a", Comparisons.Type.IN, Arrays.asList("a", "b", "c"), Boolean.TRUE},
        { "a IN {b,c,a}", "field", "a", Comparisons.Type.IN, Arrays.asList("c", "b", "a"), Boolean.TRUE},
        { "a IN {b,a,c}", "field", "a", Comparisons.Type.IN, Arrays.asList("b", "a", "c"), Boolean.TRUE},
        { "a IN {b,c}", "field", "a", Comparisons.Type.IN, Arrays.asList("b", "c"), Boolean.FALSE},
        { "a IN {}", "field", "a", Comparisons.Type.IN, Collections.emptyList(), Boolean.FALSE},
        { "a IN NULL", "field", "a", Comparisons.Type.IN, null, null},
        { "a IN {null}", "field", "a", Comparisons.Type.IN, Collections.singletonList(null), null},
        { "a IN {null, null}", "field", "a", Comparisons.Type.IN, Arrays.asList(null, null), null},
        { "a IN {null, a, null}", "field", "a", Comparisons.Type.IN, Arrays.asList(null, "a", null), Boolean.TRUE},
        { "a IN a", "field", "a", Comparisons.Type.IN, "a", null},
        {"\\xde\\xad\\xc0\\xde IN {\\xde\\xad\\xc0\\xde, \\x1e\\xe7}", "bytes_field", DEADC0DE, Comparisons.Type.IN, Arrays.asList(DEADC0DE, LEET), Boolean.TRUE},
        {"\\xde\\xad\\xc0\\xde IN {\\x1e\\xe7}", "bytes_field", DEADC0DE, Comparisons.Type.IN, Arrays.asList(LEET), Boolean.FALSE},
        { "{a} EQUALS {a}", "repeat_me", Collections.singletonList("a"), Comparisons.Type.EQUALS, Collections.singletonList("a"), Boolean.TRUE},
        { "{b} EQUALS {a}", "repeat_me", Collections.singletonList("b"), Comparisons.Type.EQUALS, Collections.singletonList("a"), Boolean.FALSE},
        { "{b} NOT_EQUALS {a}", "repeat_me", Collections.singletonList("b"), Comparisons.Type.NOT_EQUALS, Collections.singletonList("a"), Boolean.TRUE},
        { "{} EQUALS {}", "repeat_me", Collections.emptyList(), Comparisons.Type.EQUALS, Collections.emptyList(), Boolean.TRUE},
        { "{a} EQUALS {}", "repeat_me", Collections.singletonList("a"), Comparisons.Type.EQUALS, Collections.emptyList(), Boolean.FALSE},
        { "{a} STARTS_WITH {a}", "repeat_me", Collections.singletonList("a"), Comparisons.Type.STARTS_WITH, Collections.singletonList("a"), Boolean.TRUE},
        { "{a, b} STARTS_WITH {a}", "repeat_me", Arrays.asList("a", "b"), Comparisons.Type.STARTS_WITH, Collections.singletonList("a"), Boolean.TRUE},
        { "{a, b, c} STARTS_WITH {a, b}", "repeat_me", Arrays.asList("a", "b", "c"), Comparisons.Type.STARTS_WITH, Arrays.asList("a", "b"), Boolean.TRUE},
        { "{b, a} STARTS_WITH {a}", "repeat_me", Arrays.asList("b", "a"), Comparisons.Type.STARTS_WITH, Collections.singletonList("a"), Boolean.FALSE},
        { "{} STARTS_WITH {}", "repeat_me", Collections.emptyList(), Comparisons.Type.STARTS_WITH, Collections.emptyList(), Boolean.TRUE},
        { "{a} STARTS_WITH {}", "repeat_me", Collections.singletonList("a"), Comparisons.Type.STARTS_WITH, Collections.emptyList(), Boolean.TRUE},
        { "{a, b} STARTS_WITH {}", "repeat_me", Arrays.asList("a", "b"), Comparisons.Type.STARTS_WITH, Collections.emptyList(), Boolean.TRUE},
        { "{} IS NULL", "repeat_me", Collections.emptyList(), Comparisons.Type.IS_NULL, null, Boolean.FALSE},
        { "null TEXT_CONTAINS_ALL [\"parents\"]", "field", null, Comparisons.Type.TEXT_CONTAINS_ALL, Collections.singletonList("parents"), null},
        { "null TEXT_CONTAINS_ALL \"parents\"", "field", null, Comparisons.Type.TEXT_CONTAINS_ALL, "parents", null},
        { "romeo_and_juliet TEXT_CONTAINS_ALL [\"parents\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, Collections.singletonList("parents"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL \"parents\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, "parents", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL \"parents\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, "parents", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL [\"television\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, Collections.singletonList("television"), Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL \"television\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, "television", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL [\"parents\", \"television\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, Arrays.asList("parents", "television"), Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL \"parents television\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, "parents television", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL [\"computer\", \"television\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, Arrays.asList("computer", "television"), Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL \"computer television\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, "computer television", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL \"Civil blood makes civil hands unclean!\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, "Civil blood makes civil hands unclean!", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL \"\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, "", null},
        { "romeo_and_juliet TEXT_CONTAINS_ALL [\"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, Collections.singletonList(""), null},
        { "romeo_and_juliet TEXT_CONTAINS_ALL [\"\", \"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL, Arrays.asList("", ""), null},
        { "null TEXT_CONTAINS_ALL_WITHIN(3) [\"parents\", \"parents\", \"parents\"]", "field", null, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("parents", "parents", "parents"), null},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(1) \"parents\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, "parents", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(1) \"television\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, "television", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(2) [\"parents\", \"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("parents", ""), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(3) [\"parents\", \"parents\", \"parents\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("parents", "parents", "parents"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(7) \" Civil blood makes , civil hAnDS unclean. \"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, " Civil blood makes civil hAnDS unclean. ", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(6) \"blood Civil civil makes hAnDS unclean\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, "blood Civil civil makes hAnDS unclean", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(4) \"blood civil hAnDS unclean\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, "blood civil hAnDS unclean", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(4) \"where civil hAnDS unclean\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, "blood civil hAnDS unclean", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(6) [\"civil\", \"blood\", \"makes\", \"civil\", \"\", \"unclean\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("civil", "blood", "makes", "civil", "", "unclean"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(5) [\"civil\", \"blood\", \"makes\", \"\", \"unclean\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("civil", "blood", "makes", "", "unclean"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(4) [\"civil\", \"blood\", \"\", \"unclean\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("civil", "blood", "", "unclean"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(4) [\"civil\", \"where\", \"\", \"unclean\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("civil", "where", "", "unclean"), Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(0) []", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Collections.emptyList(), null},
        { "romeo_and_juliet TEXT_CONTAINS_ALL_WITHIN(4) [\"\", \"\", \"\", \"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, Arrays.asList("", "", "", ""), null},
        { "null TEXT_CONTAINS_ANY [\"parents\"]", "field", null, Comparisons.Type.TEXT_CONTAINS_ANY, Collections.singletonList("parents"), null},
        { "null TEXT_CONTAINS_ANY \"parents\"", "field", null, Comparisons.Type.TEXT_CONTAINS_ANY, "parents", null},
        { "romeo_and_juliet TEXT_CONTAINS_ANY [\"television\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, Collections.singletonList("television"), Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY \"television\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, "television", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY [\"parents\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, Collections.singletonList("parents"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY \"parents\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, "parents", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY [\"parents\", \"television\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, Arrays.asList("parents", "television"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY \"parents television\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, "parents television", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY [\"computer\", \"television\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, Arrays.asList("computer", "television"), Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY \"computer television\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, "computer television", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_ANY \"\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, "", null},
        { "romeo_and_juliet TEXT_CONTAINS_ANY [\"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, Collections.singletonList(""), null},
        { "romeo_and_juliet TEXT_CONTAINS_ANY [\"\", \"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ANY, Arrays.asList("", ""), null},
        { "null TEXT_CONTAINS_PHRASE \"Civil blood makes civil hands unclean\"", "field", null, Comparisons.Type.TEXT_CONTAINS_PHRASE, "Civil hands make civil hands unclean", null},
        { "null TEXT_CONTAINS_PHRASE [\"civil\", \"blood\", \"makes\"]", "field", null, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("civil", "blood", "makes"), null},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE \" Civil blood makes , civil hAnDS unclean. \"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_ALL_WITHIN, " Civil blood makes civil hAnDS unclean. ", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE \"blood Civil civil makes hAnDS unclean\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, "blood Civil civil makes hAnDS unclean", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE [\"civil\", \"blood\", \"makes\", \"civil\", \"\", \"unclean\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("civil", "blood", "makes", "civil", "", "unclean"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE [\"civil\", \"blood\", \"makes\", \"civil\", \"unclean\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("civil", "blood", "makes", "civil", "unclean"), Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE [\"\", \"\", \"\", \"civil\", \"blood\", \"makes\", \"civil\", \"\", \"unclean\", \"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("", "", "", "civil", "blood", "makes", "civil", "", "unclean", ""), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE \"both alike in dignity in fair verona\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, "both alike in dignity in fair verona", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE ssl_error", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, "An SSL error has occurred and a secure connection to the server cannot be made.", Boolean.FALSE},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE []", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, Collections.emptyList(), null},
        { "romeo_and_juliet TEXT_CONTAINS_PHRASE [\"\", \"\", \"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("", "", "", ""), null},
        { "french TEXT_CONTAINS_PHRASE \"après deux Napoléons\"", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PHRASE, "après deux Napoléons", Boolean.TRUE},
        { "french TEXT_CONTAINS_PHRASE \"apres deux napoleons\"", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PHRASE, "apres deux napoleons", Boolean.TRUE},
        { "french TEXT_CONTAINS_PHRASE [\"france\", \"\", \"recu\", \"un\", \"thiers\"]", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("france", "", "recu", "un", "thiers"), Boolean.TRUE},
        { "french TEXT_CONTAINS_PHRASE [\"france\", \"\", \"recu\", \"un\", \"thiers\", \"\"]", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("france", "", "recu", "un", "thiers", ""), Boolean.TRUE},
        { "french TEXT_CONTAINS_PHRASE [\"france\", \"\", \"recu\", \"un\", \"Thiers\"]", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PHRASE, Arrays.asList("france", "", "recu", "un", "Thiers"), Boolean.FALSE},
        { "\"a b a b a b c\" TEXT_CONTAINS_PHRASE \"a b a b c\"", "field", "a b a b a b c", Comparisons.Type.TEXT_CONTAINS_PHRASE, "a b a b c", Boolean.TRUE},
        { "\"a b a c a b c\" TEXT_CONTAINS_PHRASE \"a b a b c\"", "field", "a b a c a b c", Comparisons.Type.TEXT_CONTAINS_PHRASE, "a b a b c", Boolean.FALSE},
        { "null TEXT_CONTAINS_PREFIX [\"par\"]", "field", null, Comparisons.Type.TEXT_CONTAINS_PREFIX, Collections.singletonList("parents"), null},
        { "null TEXT_CONTAINS_PREFIX \"par\"", "field", null, Comparisons.Type.TEXT_CONTAINS_PREFIX, "parents", null},
        { "romeo_and_juliet TEXT_CONTAINS_PREFIX [\"parents\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PREFIX, Collections.singletonList("parents"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PREFIX \"parents\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PREFIX, "parents", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PREFIX [\"par\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PREFIX, Collections.singletonList("parents"), Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PREFIX \"par\"", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PREFIX, "parents", Boolean.TRUE},
        { "romeo_and_juliet TEXT_CONTAINS_PREFIX [\"\"]", "field", TextSamples.ROMEO_AND_JULIET_PROLOGUE, Comparisons.Type.TEXT_CONTAINS_PREFIX, Collections.singletonList(""), null},
        { "french TEXT_CONTAINS_PREFIX \"aprè\"", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PREFIX, "aprè", Boolean.TRUE},
        { "french TEXT_CONTAINS_PREFIX \"apre\"", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PREFIX, "apre", Boolean.TRUE},
        { "french TEXT_CONTAINS_PREFIX [\"aprè\"]", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PREFIX, Collections.singletonList("aprè"), Boolean.FALSE},
        { "french TEXT_CONTAINS_PREFIX [\"apre\"]", "field", TextSamples.FRENCH, Comparisons.Type.TEXT_CONTAINS_PREFIX, Collections.singletonList("apre"), Boolean.TRUE},
        { "70000000-0001-0002-0003-000000000004 < 80000000-0001-0002-0003-000000000004", "uuid_field", UUID.fromString("70000000-0001-0002-0003-000000000004"), Comparisons.Type.LESS_THAN, UUID.fromString("80000000-0001-0002-0003-000000000004"), Boolean.TRUE }
    };

    @Test
    public void testComparisons() throws Exception {
        for (Object[] test : COMPARISON_TESTS) {
            testComparison((String)test[0], (String)test[1], test[2], (Comparisons.Type)test[3], test[4], (Boolean)test[5]);
        }
    }

    @SuppressWarnings("unchecked")
    protected void testComparison(String name, String field, Object val1, Comparisons.Type type, Object val2, Boolean expected) throws Exception {
        final TestScalarFieldAccess.Builder rec = createRecord(field, val1);
        try {
            final Comparisons.Comparison comparison;
            switch (type) {
                case IS_NULL:
                case NOT_NULL:
                    comparison = new Comparisons.NullComparison(type);
                    break;
                case IN:
                    if (!(val2 instanceof List) && val2 != null) {
                        comparison = new Comparisons.SimpleComparison(type, val2);
                        break;
                    }
                    final List<?> listComparand = (List<?>) val2;
                    if (listComparand == null || listComparand.stream().anyMatch(o -> o == null)) {
                        try {
                            assertThrows(name, NullPointerException.class,
                                    () -> new Comparisons.ListComparison(Comparisons.Type.IN, listComparand));
                            return;
                        } catch (IllegalArgumentException e) {
                            // When run inside IntelliJ
                            if (e.getMessage().contains("@Nonnull") && val2 == null) {
                                return;
                            } else {
                                throw e;
                            }
                        }
                    }
                    comparison = new Comparisons.ListComparison(Comparisons.Type.IN, listComparand);
                    break;
                case TEXT_CONTAINS_ALL:
                case TEXT_CONTAINS_ANY:
                case TEXT_CONTAINS_PHRASE:
                case TEXT_CONTAINS_PREFIX:
                    if (val2 instanceof String) {
                        comparison = new Comparisons.TextComparison(type, (String) val2, DefaultTextTokenizer.NAME, DefaultTextTokenizer.NAME);
                    } else {
                        comparison = new Comparisons.TextComparison(type, (List<String>) val2, DefaultTextTokenizer.NAME, DefaultTextTokenizer.NAME);
                    }
                    break;
                case TEXT_CONTAINS_ALL_WITHIN:
                    if (val2 instanceof String) {
                        int maxDistance = ((String)val2).split(" ").length;
                        comparison = new Comparisons.TextWithMaxDistanceComparison((String) val2, maxDistance, DefaultTextTokenizer.NAME, DefaultTextTokenizer.NAME);
                    } else {
                        int maxDistance = ((List<?>)val2).size();
                        comparison = new Comparisons.TextWithMaxDistanceComparison((List<String>) val2, maxDistance, DefaultTextTokenizer.NAME, DefaultTextTokenizer.NAME);
                    }
                    break;
                default:
                    if (val2 instanceof List) {
                        comparison = new Comparisons.ListComparison(type, (List) val2);
                    } else {
                        comparison = new Comparisons.SimpleComparison(type, val2);
                    }
                    break;
            }
            final QueryComponent qc = new FieldWithComparison(field, comparison);
            assertEquals(expected, evaluate(qc, rec.build()), name);
        } catch (Exception e) {
            if (type == Comparisons.Type.IN && !(val2 instanceof List) && e instanceof RecordCoreException && e.getMessage().contains("non-list")) {
                return;
            }
            throw new AssertionError(name + " Threw: " + e.getMessage(), e);
        }
    }

    protected void testParameterComparison(String name, String field, Object val1, Comparisons.Type type, Object val2, Boolean expected) throws Exception {
        if (type.isUnary()) {
            assertThrows(name, RecordCoreException.class, () -> new Comparisons.ParameterComparison(type, "fooParam"));
            return;
        }
        final TestScalarFieldAccess rec = createRecord(field, val1).build();
        try {
            final Comparisons.Comparison comparison = new Comparisons.ParameterComparison(type, "fooParam");
            final QueryComponent qc = new FieldWithComparison(field, comparison);
            final Bindings bindings = Bindings.newBuilder()
                    .set("fooParam", val2)
                    .build();
            if (val1 != null && val2 != null && (type == Comparisons.Type.IN  && !(val2 instanceof List) || type.name().startsWith("TEXT_"))) {
                assertThrows(name, RecordCoreException.class,
                        () -> evaluate(qc, bindings, rec));
            } else {
                assertEquals(expected, evaluate(qc, bindings, rec), name);
            }
        } catch (Exception e) {
            throw new AssertionError(name + " Threw: " + e.getMessage(), e);
        }
    }

    @Nonnull
    private TestScalarFieldAccess.Builder createRecord(String field, Object val1) {
        final TestScalarFieldAccess.Builder rec = TestScalarFieldAccess.newBuilder();
        if (val1 != null) {
            Descriptors.FieldDescriptor fieldDescriptor = rec.getDescriptorForType().findFieldByName(field);
            if (val1 instanceof byte[]) {
                rec.setField(fieldDescriptor, ByteString.copyFrom((byte[])val1));
            } else if (val1 instanceof UUID) {
                rec.setField(fieldDescriptor, TupleFieldsHelper.toProto((UUID)val1));
            } else if (val1 instanceof List) {
                for (Object val : (List)val1) {
                    rec.addRepeatedField(fieldDescriptor, val);
                }
            } else {
                rec.setField(fieldDescriptor, val1);
            }
        }
        return rec;
    }

    @Test
    public void testEmpties() throws Exception {
        final ExpressionTestsProto.NestedField.Builder rec = ExpressionTestsProto.NestedField.newBuilder();
        final QueryComponent isEmpty = new EmptyComparison("repeated_field", true);
        final QueryComponent notEmpty = new EmptyComparison("repeated_field", false);
        assertTrue(evaluate(isEmpty, rec.build()));
        assertFalse(evaluate(notEmpty, rec.build()));

        rec.addRepeatedField("one");
        rec.addRepeatedField("two");

        assertFalse(evaluate(isEmpty, rec.build()));
        assertTrue(evaluate(notEmpty, rec.build()));
    }

    @Test
    public void testParameterComparisons() throws Exception {
        for (Object[] test : COMPARISON_TESTS) {
            testParameterComparison(
                    (String) test[0], (String) test[1], test[2], (Comparisons.Type) test[3], test[4], (Boolean) test[5]);
        }
    }

    @Test
    public void testParameterComparisonsSimple() throws Exception {
        final TestScalarFieldAccess rec = TestScalarFieldAccess.newBuilder().setField("abc").build();
        final QueryComponent equalsP1 = new FieldWithComparison("field", new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "p1"));
        final QueryComponent notEqualsP2 = new FieldWithComparison("field", new Comparisons.ParameterComparison(Comparisons.Type.NOT_EQUALS, "p2"));
        final Bindings b1 = Bindings.newBuilder()
                .set("p1", "abc")
                .set("p2", "xyz")
                .build();
        assertTrue(evaluate(equalsP1, b1, rec));
        assertTrue(evaluate(notEqualsP2, b1, rec));
        final Bindings b2 = Bindings.newBuilder()
                .set("p1", "foo")
                .set("p2", "bar")
                .build();
        assertFalse(evaluate(equalsP1, b2, rec));
        assertTrue(evaluate(notEqualsP2, b2, rec));
    }

    @Test
    @Tag(Tags.Slow)
    public void testValidation() {
        final List<QueryComponent> validFilterBase = Arrays.asList(
                Query.field("nesty").isNull(),
                Query.field("repeated_nesty").isEmpty(),
                Query.field("regular_old_field").isNull(),
                Query.field("regular_old_field").greaterThan("this string"),
                Query.field("regular_old_field").startsWith("blah"),
                Query.field("regular_old_field").in(Arrays.asList("a", "b", "c")),
                Query.field("repeated_field").oneOfThem().in(Arrays.asList("d", "e", "f")),
                new FieldWithComparison("repeated_field", new Comparisons.ListComparison(Comparisons.Type.STARTS_WITH, Arrays.asList("the", "start"))),
                Query.field("repeated_field").isEmpty(),
                Query.field("repeated_field").oneOfThem().lessThan("this string"),
                Query.field("repeated_field").equalsValue(Arrays.asList("a", "b", "c"))
        );
        final List<QueryComponent> invalidFilterBase = Arrays.asList(
                Query.field("not_in_message_type").isNull(),
                Query.field("nesty").isEmpty(),
                Query.field("repeated_nesty").isNull(),
                Query.field("regular_old_field").isEmpty(),
                Query.field("regular_old_field").greaterThan(42),
                Query.field("regular_old_field").oneOfThem().in(Arrays.asList("g", "h", "i")),
                Query.field("regular_old_field").in(Arrays.asList(1, 2, 3)),
                Query.field("repeated_field").isNull(),
                Query.field("repeated_field").oneOfThem().lessThan(47),
                Query.field("repeated_field").equalsValue("a"),
                Query.field("repeated_field").startsWith("qwerty"),
                new FieldWithComparison("repeated_field", new Comparisons.ListComparison(Comparisons.Type.STARTS_WITH, Arrays.asList(0, 1))),
                Query.field("repeated_field").in(Arrays.asList("j", "k", "l")),
                Query.field("repeated_field").oneOfThem().in(Arrays.asList(4, 5, 6))
        );

        final List<Function<QueryComponent, QueryComponent>> validTransforms = Arrays.asList(
                Query::not,
                filter -> Query.field("nesty").matches(filter),
                filter -> Query.field("repeated_nesty").oneOfThem().matches(filter)
        );
        final List<Function<QueryComponent, QueryComponent>> invalidTransforms = Arrays.asList(
                filter -> Query.field("not_in_message").matches(filter),
                filter -> Query.field("regular_field").matches(filter),
                filter -> Query.field("nesty").oneOfThem().matches(filter),
                filter -> Query.field("repeated_nesty").matches(filter)
        );

        List<QueryComponent> validFilters = new ArrayList<>(validFilterBase);
        List<QueryComponent> invalidFilters = new ArrayList<>(invalidFilterBase);

        for (int i = 0; i < 2; i++) {
            final int validSize = validFilters.size();
            final int invalidSize = invalidFilters.size();

            // Run all valid filters through the valid and invalid transforms.
            validFilters.addAll(validTransforms.stream().flatMap(transform -> validFilters.stream().limit(validSize).map(transform)).collect(Collectors.toList()));
            invalidFilters.addAll(invalidTransforms.stream().flatMap(transform -> validFilters.stream().limit(validSize).map(transform)).collect(Collectors.toList()));
            invalidFilters.addAll(invalidTransforms.stream().flatMap(transform -> invalidFilters.stream().limit(invalidSize).map(transform)).collect(Collectors.toList()));

            // Combine the transforms with Ors and Ands.
            // Limit by 2 to avoid *too* big an explosion.
            validFilters.addAll(validFilters.stream().limit(validSize / 2).flatMap(filter1 ->
                    validFilters.stream().limit(validSize).flatMap(filter2 -> Stream.of(Query.and(filter1, filter2), Query.or(filter1, filter2)))
            ).collect(Collectors.toList()));
            invalidFilters.addAll(validFilters.stream().limit(validSize / 2).flatMap(filter1 ->
                    invalidFilters.stream().limit(invalidSize).flatMap(filter2 -> Stream.of(Query.and(filter1, filter2), Query.and(filter2, filter1), Query.or(filter1, filter2), Query.or(filter2, filter1)))
            ).collect(Collectors.toList()));
        }

        for (QueryComponent filter : validFilters) {
            try {
                filter.validate(ExpressionTestsProto.NestedField.getDescriptor());
            } catch (Query.InvalidExpressionException | IllegalArgumentException e) {
                throw new RuntimeException("Valid filter " + filter + " was marked as invalid", e);
            }
        }
        for (QueryComponent filter : invalidFilters) {
            try {
                filter.validate(ExpressionTestsProto.NestedField.getDescriptor());
                fail("Invalid filter " + filter + " was marked as valid");
            } catch (Query.InvalidExpressionException | RecordCoreException e) {
                // pass
            }
        }
    }

    @Test
    public void async() {
        // Not async.
        assertFalse(Query.field("blah").greaterThan(100).isAsync());
        assertFalse(Query.field("blah").matches(Query.field("sub-field").isEmpty()).isAsync());
        assertFalse(Query.field("blah").oneOfThem().lessThan(20).isAsync());
        assertFalse(Query.field("blah").oneOfThem().matches(Query.field("sub-field").greaterThanOrEquals(10)).isAsync());
        assertFalse(Query.field("blah").isEmpty().isAsync());
        assertFalse(Query.and(
                Query.field("blah").isNull(),
                Query.field("blah").matches(Query.field("sub-field").greaterThanOrEquals(10)))
                .isAsync());
        assertFalse(Query.or(
                Query.field("blah").isNull(),
                Query.field("blah").matches(Query.field("sub-field").greaterThanOrEquals(10)))
                .isAsync());
        assertFalse(Query.not(Query.field("blah").isNull()).isAsync());

        // Async.
        assertTrue(Query.rank("blah").equalsValue(42L).isAsync());
        assertTrue(Query.field("blah").matches(Query.rank("sub-field").notEquals(10L)).isAsync());
        assertTrue(Query.not(Query.field("blah").matches(Query.rank("sub-field").lessThan(5L))).isAsync());
        assertTrue(Query.and(
                Query.rank("asdf").greaterThanOrEquals(5L),
                Query.field("blah").matches(Query.rank("sub-field").lessThan(5L))
        ).isAsync());
        assertTrue(Query.and(
                Query.field("asdf").greaterThanOrEquals(5L),
                Query.field("blah").matches(Query.rank("sub-field").lessThan(5L))
        ).isAsync());
        assertTrue(Query.or(
                Query.rank("asdf").greaterThanOrEquals(5L),
                Query.field("blah").matches(Query.rank("sub-field").lessThan(5L))
        ).isAsync());
        assertTrue(Query.or(
                Query.field("asdf").greaterThanOrEquals(5L),
                Query.field("blah").matches(Query.rank("sub-field").lessThan(5L))
        ).isAsync());
    }
}
