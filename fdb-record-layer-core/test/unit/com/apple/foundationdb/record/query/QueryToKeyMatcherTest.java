/*
 * QueryToKeyMatcherTest.java
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

package com.apple.foundationdb.record.query;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.expressions.Field;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.query.QueryToKeyMatcher.Match;
import static com.apple.foundationdb.record.query.QueryToKeyMatcher.MatchType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link QueryToKeyMatcher}.
 */
public class QueryToKeyMatcherTest {

    @Test
    public void testSingleFieldEquality() {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(queryField("a").equalsValue(7));
        Match match = matcher.matches(keyField("a"));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(7), match.getEquality());


        match = matcher.matches(concatenateFields("a", "b"));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(7), match.getEquality());


        assertNoMatch(matcher.matches(keyField("b")));
        assertNoMatch(matcher.matches(keyField("a", FanType.FanOut)));
        assertNoMatch(matcher.matches(keyField("a", FanType.Concatenate)));
        assertNoMatch(matcher.matches(keyField("b").nest("a")));
        assertNoMatch(matcher.matches(keyField("a").nest("b")));

        assertNoMatch(matcher.matches(concatenateFields("b", "a")));
    }

    @Test
    public void testMatchKeyWithValue() {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(
                Query.and(
                        queryField("f1").equalsValue(7),
                        queryField("f2").equalsValue(11)));

        Match match = matcher.matches(keyWithValue(concatenateFields("f1", "f2", "f3", "f4"), 2));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.concatenate(7, 11), match.getEquality());
    }

    @Test
    public void testMatchWithFunctionExpression() {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(queryField("f1").equalsValue("hello!"));
        Match match = matcher.matches(keyWithValue(function("nada", concatenateFields("f1", "f2", "f3")), 1));
        assertEquals(MatchType.NO_MATCH, match.getType());
    }

    @Test
    public void testMatchWithValueExpression() {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(queryField("f1").equalsValue("hello!"));
        Match match = matcher.matches(value(4));
        assertEquals(MatchType.NO_MATCH, match.getType());
    }

    @Test
    public void testSingleNestedFieldEquality() {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(queryField("a").matches(queryField("ax").equalsValue(10)));
        Match match = matcher.matches(keyField("a").nest("ax"));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(10), match.getEquality());

        match = matcher.matches(concat(keyField("a").nest("ax"), keyField("b")));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(10), match.getEquality());

        match = matcher.matches(keyField("a").nest(concat(keyField("ax"), keyField("b"))));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(10), match.getEquality());

        final Match match2 = matcher.matches(keyField("a"));
        assertNoMatch(match2);
        assertNoMatch(matcher.matches(keyField("a", FanType.FanOut)));
        assertNoMatch(matcher.matches(keyField("a", FanType.Concatenate)));
        assertNoMatch(matcher.matches(keyField("a", FanType.FanOut).nest("ax")));
        assertNoMatch(matcher.matches(keyField("a").nest("ax", FanType.FanOut)));
        assertNoMatch(matcher.matches(keyField("a").nest("ax", FanType.Concatenate)));
        assertNoMatch(matcher.matches(keyField("b").nest("ax")));
        assertNoMatch(matcher.matches(keyField("a").nest("bx")));

        assertNoMatch(matcher.matches(concat(keyField("b").nest("ax"), keyField("a").nest("ax"))));
        assertNoMatch(matcher.matches(keyField("a").nest(concat(keyField("bx"), keyField("ax")))));
    }

    @Test
    public void testThen() {
        assertEquality(MatchType.EQUALITY,
                queryField("a").equalsValue(1),
                concatenateFields("a", "b"));

        assertEquality(MatchType.NO_MATCH,
                queryField("a").equalsValue(1),
                concatenateFields("b", "a"));

        assertEquality(MatchType.NO_MATCH,
                queryField("a").equalsValue(1),
                concatenateFields("a", "b"),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.EQUALITY,
                queryField("a").oneOfThem().equalsValue(1),
                concat(keyField("a", FanType.FanOut), keyField("b")));

        assertEquality(MatchType.NO_MATCH,
                queryField("a").oneOfThem().equalsValue(1),
                concat(keyField("b"), keyField("a", FanType.FanOut)));

        assertEquality(MatchType.NO_MATCH,
                queryField("a").oneOfThem().equalsValue(1),
                concat(keyField("a", FanType.FanOut), keyField("b")),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.EQUALITY,
                new RecordTypeKeyComparison("ErsatzRecordType"),
                concat(recordType(), keyField("a")));

        assertEquality(MatchType.NO_MATCH,
                new RecordTypeKeyComparison("ErsatzRecordType"),
                concat(keyField("a"), recordType()));

        assertEquality(MatchType.NO_MATCH,
                new RecordTypeKeyComparison("ErsatzRecordType"),
                concat(recordType(), keyField("a")),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);
    }

    @Test
    public void testQueryAndPatterns() {
        assertEquality(MatchType.NO_MATCH,
                queryField("p").matches(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2))),
                keyField("p").nest(concatenateFields("a", "c", "b")));

        assertEquality(MatchType.NO_MATCH,
                queryField("p").matches(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2))),
                keyField("p").nest(keyField("a"), keyField("q").nest(keyField("c"), keyField("d")), keyField("b")));

        assertEquality(MatchType.EQUALITY,
                queryField("p").matches(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2))),
                keyField("p").nest(keyField("a"), keyField("b"), keyField("q").nest(keyField("c"), keyField("d"))));

        assertEquality(MatchType.NO_MATCH,
                queryField("p").matches(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2))),
                keyField("p").nest(keyField("a"), keyField("b"), keyField("q").nest(keyField("c"), keyField("d"))),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.EQUALITY,
                queryField("p").matches(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2),
                                queryField("c").equalsValue(3))),
                keyField("p").nest(concatenateFields("c", "b", "a", "extra")));

        assertEquality(MatchType.NO_MATCH,
                queryField("p").matches(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2),
                                queryField("c").equalsValue(3))),
                keyField("p").nest(concatenateFields("c", "b", "a", "extra")),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.EQUALITY,
                queryField("p").matches(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2),
                                queryField("c").equalsValue(3))),
                keyField("p").nest(concatenateFields("c", "b")),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.INEQUALITY,
                Query.and(
                        queryField("a").lessThanOrEquals(1),
                        queryField("b").equalsValue(2),
                        queryField("c").equalsValue(3)),
                concatenateFields("c", "b", "a"));

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("a").lessThanOrEquals(1),
                        queryField("b").equalsValue(2)),
                concatenateFields("a", "b", "c", "d"));

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("a").equalsValue(1),
                        queryField("b").equalsValue(2),
                        queryField("c").equalsValue(3),
                        queryField("DoesNotExist").lessThan(4)),
                concatenateFields("c", "b", "a"));

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("a").equalsValue(1),
                        queryField("b").equalsValue(2),
                        queryField("c").equalsValue(3),
                        queryField("DoesNotExist").lessThan(4)),
                concatenateFields("c", "b", "a"),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("a").isNull(),
                        queryField("b").equalsValue(2)),
                concatenateFields("a", "b"));

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("p").matches(queryField("c").equalsValue(1)),
                        queryField("b").equalsValue(2)),
                concat(
                        keyField("p").nest(keyField("c")),
                        keyField("b")));

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("p").matches(queryField("c").equalsValue(1)),
                        queryField("b").equalsValue(2),
                        queryField("a").equalsValue(1)),
                concat(
                        keyField("p").nest(keyField("c")),
                        keyField("b")),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("p").matches(queryField("d").equalsValue(1)),
                        queryField("b").equalsValue(2)),
                concat(
                        keyField("p").nest(keyField("c")),
                        keyField("b")));

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("q").matches(queryField("c").equalsValue(1)),
                        queryField("b").equalsValue(2)),
                concat(
                        keyField("p").nest(keyField("c")),
                        keyField("b")));

        assertEquality(MatchType.INEQUALITY,
                Query.and(
                        queryField("p").matches(queryField("c").equalsValue(1)),
                        queryField("b").lessThanOrEquals(2)),
                concat(
                        keyField("p").nest(keyField("c")),
                        keyField("b")));

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("p").matches(queryField("c").lessThanOrEquals(1)),
                        queryField("b").equalsValue(2)),
                concat(
                        keyField("p").nest(keyField("c")),
                        keyField("b")));

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("p").matches(queryField("c1").equalsValue(1)),
                        queryField("p").matches(queryField("c2").equalsValue(2))),
                concat(
                        keyField("p").nest(keyField("c1")),
                        keyField("p").nest(keyField("c2"))));

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("a").equalsValue(1),
                        queryField("b").equalsValue(2)
                ),
                keyField("a"));

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("a").equalsValue(1),
                        queryField("b").equalsValue(2)
                ),
                keyField("a"),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("a").equalsValue(1),
                        queryField("b").equalsValue(2)
                ),
                keyField("b"));

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("a").equalsValue(1),
                        queryField("b").equalsValue(2)
                ),
                keyField("b"),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("a").equalsValue(1),
                        new RecordTypeKeyComparison("ErsatzRecordType")
                ),
                concat(recordType(), keyField("a")));

        assertEquality(MatchType.NO_MATCH,
                Query.and(
                        queryField("a").equalsValue(1),
                        new RecordTypeKeyComparison("ErsatzRecordType")
                ),
                recordType());

        assertEquality(MatchType.EQUALITY,
                Query.and(
                        queryField("a").equalsValue(1),
                        new RecordTypeKeyComparison("ErsatzRecordType")
                ),
                recordType(),
                QueryToKeyMatcher.MatchingMode.COVER_KEY);
    }

    @Test
    public void testOneOfThem() {
        QueryToKeyMatcher matcher = new QueryToKeyMatcher(queryField("a").oneOfThem().equalsValue(7));
        Match match = matcher.matches(keyField("a", FanType.FanOut));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(7), match.getEquality());

        matcher = new QueryToKeyMatcher(queryField("p").matches(queryField("a").oneOfThem().equalsValue(7)));
        match = matcher.matches(keyField("p").nest(keyField("a", FanType.FanOut)));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(7), match.getEquality());

        matcher = new QueryToKeyMatcher(queryField("g").matches(queryField("p").oneOfThem().matches(queryField("a").equalsValue(7))));
        match = matcher.matches(keyField("g").nest(keyField("p", FanType.FanOut).nest(keyField("a"))));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.scalar(7), match.getEquality());

        matcher = new QueryToKeyMatcher(Query.and(
                queryField("a").equalsValue(10),
                queryField("g").matches(queryField("p").oneOfThem().matches(queryField("a").equalsValue(7)))));
        match = matcher.matches(concat(
                keyField("a"),
                keyField("g").nest(keyField("p", FanType.FanOut).nest(keyField("a"))),
                keyField("b")));
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(Key.Evaluated.concatenate(10, 7), match.getEquality());

        assertNoMatch(queryField("a").oneOfThem().matches(queryField("ax").greaterThan(8)),
                keyField("a", FanType.FanOut));

        assertNoMatch(queryField("p").matches(queryField("a").oneOfThem().matches(queryField("ax").greaterThan(8))),
                keyField("p").nest(keyField("a", FanType.FanOut)));
    }

    @Test
    public void testTemporarilyUnsupported() {
        // This is a holder test to make sure we don't forget to test things when we add support for them, and
        // to make sure they correctly throw errors here
        // Ideally these match correctly once implemented
        assertInvalid(Query.and(queryField("a").equalsValue(3), queryField("b").isEmpty()), concatenateFields("a", "b"));

        // Eventually we want this to match.
        assertInvalid(Query.and(queryField("a").lessThan(3), queryField("a").greaterThan(0)), concatenateFields("a", "b"));
        assertInvalid(Query.or(queryField("a").equalsValue(3), queryField("b").equalsValue(4)), concatenateFields("a", "b"));
        assertInvalid(Query.not(queryField("a").equalsValue(3)), keyField("a"));
        assertInvalid(Query.rank("a").equalsValue(5), keyField("a"));

        assertInvalid(
                queryField("p").matches(Query.and(queryField("a").greaterThan(3),
                        Query.or(queryField("b").lessThan(4), queryField("b").greaterThan(5)))),
                keyField("p").nest(concatenateFields("a", "b")));
        assertInvalid(
                queryField("p").matches(Query.or(queryField("a").equalsValue(3), queryField("b").equalsValue(4))),
                keyField("p").nest(concatenateFields("a", "b")));
        assertInvalid(
                queryField("p").matches(Query.not(queryField("a").equalsValue(3))),
                keyField("p").nest(keyField("a")));
        assertInvalid(
                queryField("p").matches(Query.rank("a").equalsValue(5)),
                keyField("p").nest(keyField("a")));
    }

    @Test
    public void testTemporarilyNoMatch() {
        // This is a holder test to make sure we don't forget to test things when we add support for them, and
        // to make sure they return no match for now
        // Ideally these match correctly once implemented
        assertNoMatch(
                queryField("p").matches(Query.and(
                        queryField("c1").equalsValue(1),
                        queryField("c2").equalsValue(2))),
                concat(
                        keyField("p").nest(keyField("c1")),
                        keyField("p").nest(keyField("c2"))));
        assertNoMatch(
                Query.and(
                        queryField("p").matches(queryField("c1").equalsValue(1)),
                        queryField("p").matches(queryField("c2").equalsValue(2))),
                keyField("p").nest(concatenateFields("c1", "c2")));


    }

    private void assertEquality(MatchType type, QueryComponent query, KeyExpression key) {
        assertEquality(type, query, key, QueryToKeyMatcher.MatchingMode.SATISFY_QUERY);
    }

    private void assertEquality(MatchType type, QueryComponent query, KeyExpression key, QueryToKeyMatcher.MatchingMode mode) {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(query, mode);
        assertEquals(type, matcher.matches(key).getType());
    }

    private void assertInvalid(QueryComponent query, KeyExpression key) {
        assertThrows(Query.InvalidExpressionException.class, () -> {
            final QueryToKeyMatcher matcher = new QueryToKeyMatcher(query);
            matcher.matches(key);
        });
    }

    private void assertNoMatch(QueryComponent query, KeyExpression key) {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(query);
        assertNoMatch(matcher.matches(key));
    }

    private void assertNoMatch(Match match) {
        assertEquals(MatchType.NO_MATCH, match.getType());
    }

    private FieldKeyExpression keyField(String name) {
        return Key.Expressions.field(name);
    }

    private FieldKeyExpression keyField(String name, FanType fanType) {
        return Key.Expressions.field(name, fanType);
    }

    private Field queryField(String name) {
        return Query.field(name);
    }

    /**
     * Function registry for {@link DoNothingFunction}.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class TestFunctionRegistry implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return Collections.singletonList(new FunctionKeyExpression.BiFunctionBuilder("nada",
                    DoNothingFunction::new));
        }
    }

    private static class DoNothingFunction extends FunctionKeyExpression {

        public DoNothingFunction(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 0;
        }

        @Override
        public int getMaxArguments() {
            return Integer.MAX_VALUE;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nonnull FDBEvaluationContext<M> context,
                                                                        @Nullable FDBRecord<M> record,
                                                                        @Nullable Message message,
                                                                        @Nonnull Key.Evaluated arguments) {
            return Collections.singletonList(arguments);
        }

        @Override
        public boolean createsDuplicates() {
            return getArguments().createsDuplicates();
        }

        @Override
        public int getColumnSize() {
            return getArguments().getColumnSize();
        }
    }
}
