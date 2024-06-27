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

import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.UnknownKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.expressions.Field;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Evaluated.scalar;
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

    @SuppressWarnings("unused") // used as arguments for parameterized test
    static Stream<Arguments> testEqualityMatches() {
        Stream<Arguments> singleFieldArgs = Stream.of(
                keyField("a"),
                concatenateFields("a", "b"),
                keyWithValue(concatenateFields("a", "b"), 1),
                keyWithValue(concatenateFields("a", "b", "c"), 1),
                keyWithValue(concatenateFields("a", "b", "c"), 2),
                keyField("b").groupBy(keyField("a")),
                keyField("c").groupBy(keyField("a"), keyField("b")),
                concatenateFields("b", "c").groupBy(keyField("a")),
                concatenateFields("c", "d").groupBy(keyField("a"), keyField("b")),
                keyWithValue(concat(keyField("a"), function("nada", concatenateFields("b", "c"))), 2),
                keyWithValue(concat(keyField("a"), function("nada", concatenateFields("b", "c"))), 3)
        ).map(key -> Arguments.of(
                queryField("a").equalsValue(7),
                key,
                scalar(7)
        ));
        Stream<Arguments> singleNestedFieldArgs = Stream.of(
                keyField("a").nest("ax"),
                concat(keyField("a").nest("ax"), keyField("b")),
                keyField("a").nest(keyField("ax"), keyField("b")),
                keyField("b").groupBy(keyField("a").nest("ax")),
                new GroupingKeyExpression(keyField("a").nest(keyField("ax"), keyField("b")), 1),
                new GroupingKeyExpression(keyField("a").nest(keyField("ax"), keyField("b")), 0)
        ).map(key -> Arguments.of(
                queryField("a").matches(queryField("ax").equalsValue(10)),
                key,
                scalar(10)
        ));
        return Stream.of(singleFieldArgs, singleNestedFieldArgs, Stream.of(
                Arguments.of(
                        queryField("a").oneOfThem().equalsValue(7),
                        keyField("a", FanType.FanOut),
                        scalar(7)
                ),
                Arguments.of(
                        queryField("p").matches(queryField("a").oneOfThem().equalsValue(7)),
                        keyField("p").nest(keyField("a", FanType.FanOut)),
                        scalar(7)
                ),
                Arguments.of(
                        queryField("g").matches(queryField("p").matches(queryField("a").oneOfThem().equalsValue(7))),
                        keyField("g").nest(keyField("p").nest(keyField("a", FanType.FanOut))),
                        scalar(7)
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(10),
                                queryField("g").matches(queryField("p").matches(queryField("a").oneOfThem().equalsValue(7)))),
                        concat(keyField("a"), keyField("g").nest(keyField("p").nest(keyField("a", FanType.FanOut)))),
                        Key.Evaluated.concatenate(10, 7)
                ),
                Arguments.of(
                        Query.and(
                                queryField("f1").equalsValue(7),
                                queryField("f2").equalsValue(11)),
                        keyWithValue(concatenateFields("f1", "f2", "f3", "f4"), 2),
                        Key.Evaluated.concatenate(7, 11)
                ),
                Arguments.of(
                        Query.keyExpression(function("nada", concatenateFields("f1", "f2", "f3"))).equalsValue("hello!"),
                        function("nada", concatenateFields("f1", "f2", "f3")),
                        scalar("hello!")
                )
        )).flatMap(Function.identity());
    }

    @ParameterizedTest(name = "testEqualityMatches[query = {0}, key = {1}")
    @MethodSource
    void testEqualityMatches(QueryComponent query, KeyExpression key, Key.Evaluated evaluated) {
        QueryToKeyMatcher matcher = new QueryToKeyMatcher(query);
        Match match = matcher.matchesSatisfyingQuery(key);
        assertEquals(MatchType.EQUALITY, match.getType());
        assertEquals(evaluated, match.getEquality());
    }

    @SuppressWarnings("unused") // used as arguments for parameterized test
    static Stream<Arguments> testNoMatch() {
        Stream<Arguments> singleFieldQueryArgs = Stream.of(
                keyField("b"),
                keyField("a", FanType.FanOut),
                keyField("a", FanType.Concatenate),
                keyField("b").nest("a"),
                keyField("a").nest("b"),
                concatenateFields("b", "a"),
                keyField("a").ungrouped(),
                keyField("a").groupBy(keyField("b")),
                concatenateFields("a", "b").ungrouped()
        ).map(key ->
                Arguments.of(queryField("a").equalsValue(7), key)
        );
        Stream<Arguments> singleNestedQueryArgs = Stream.of(
                keyField("a"),
                keyField("a", FanType.FanOut),
                keyField("a", FanType.Concatenate),
                keyField("a", FanType.FanOut).nest("ax"),
                keyField("a").nest("ax", FanType.FanOut),
                keyField("a").nest("ax", FanType.Concatenate),
                keyField("b").nest("ax"),
                keyField("a").nest("bx"),

                concat(keyField("b").nest("ax"), keyField("a").nest("ax")),
                keyField("a").nest(concat(keyField("bx"), keyField("ax")))
        ).map(key ->
                Arguments.of(queryField("a").matches(queryField("ax").equalsValue(10)), key)
        );
        return Stream.of(singleFieldQueryArgs, singleNestedQueryArgs, Stream.of(
                Arguments.of(
                        queryField("a").oneOfThem().matches(queryField("ax").greaterThan(8)),
                        keyField("a", FanType.FanOut)
                ),
                Arguments.of(
                        queryField("p").matches(queryField("a").oneOfThem().matches(queryField("ax").greaterThan(8))),
                        keyField("p").nest(keyField("a", FanType.FanOut))
                ),
                Arguments.of(
                        queryField("f1").equalsValue("hello!"),
                        keyWithValue(function("nada", concatenateFields("f1", "f2", "f3")), 1)
                ),
                Arguments.of(
                        queryField("f1").equalsValue("hello!"),
                        value(4)
                ),
                Arguments.of(
                        // Note the different arguments in the query and key expression
                        Query.keyExpression(function("nada", concatenateFields("f1", "f2", "f3"))).equalsValue("hello!"),
                        function("nada", concatenateFields("f1", "f2", "f3", "f4"))
                )
        )).flatMap(Function.identity());
    }

    @ParameterizedTest(name = "testNoMatch[query = {0}, key = {1}")
    @MethodSource
    void testNoMatch(QueryComponent query, KeyExpression key) {
        assertNoMatch(query, key);
        assertEqualityCoveringKey(MatchType.NO_MATCH, query, key);
    }

    @SuppressWarnings("unused") // used as arguments for parameterized test
    static Stream<Arguments> testQueryAndPatterns() {
        return Stream.of(
                Arguments.of(
                        queryField("a").matches(queryField("ax").equalsValue(10)),
                        keyField("a").nest(concat(keyField("ax"), keyField("b"))),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("a").equalsValue(1),
                        concatenateFields("a", "b"),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("a").equalsValue(1),
                        concatenateFields("b", "a"),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("a").oneOfThem().equalsValue(1),
                        concat(keyField("a", FanType.FanOut), keyField("b")),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("a").oneOfThem().equalsValue(1),
                        concat(keyField("b"), keyField("a", FanType.FanOut)),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        new RecordTypeKeyComparison("ErsatzRecordType"),
                        concat(recordType(), keyField("a")),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        new RecordTypeKeyComparison("ErsatzRecordType"),
                        concat(keyField("a"), recordType()),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("p").matches(
                                Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2))),
                        keyField("p").nest(concatenateFields("a", "c", "b")),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("p").matches(
                                Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2))),
                        keyField("p").nest(
                                keyField("a"),
                                keyField("q").nest(
                                        keyField("c"),
                                        keyField("d")),
                                keyField("b")),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("p").matches(
                                Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2))),
                        keyField("p").nest(
                                keyField("a"),
                                keyField("b"),
                                keyField("q").nest(keyField("c"), keyField("d"))),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("p").matches(
                                Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2),
                                        queryField("c").equalsValue(3))),
                        keyField("p").nest(concatenateFields("c", "b", "a", "extra")),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        queryField("p").matches(
                                Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2),
                                        queryField("c").equalsValue(3))),
                        keyField("p").nest(concatenateFields("c", "b")),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        queryField("p").matches(
                                Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2),
                                        queryField("c").equalsValue(3))),
                        keyField("p").nest(concatenateFields("c", "b", "a")),
                        MatchType.EQUALITY,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").lessThanOrEquals(1),
                                queryField("b").equalsValue(2),
                                queryField("c").equalsValue(3)),
                        concatenateFields("c", "b", "a"),
                        MatchType.INEQUALITY,
                        MatchType.INEQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").lessThanOrEquals(1),
                                queryField("b").equalsValue(2)),
                        concatenateFields("a", "b", "c", "d"),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2),
                                queryField("c").equalsValue(3),
                                queryField("DoesNotExist").lessThan(4)),
                        concatenateFields("c", "b", "a"),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").isNull(),
                                queryField("b").equalsValue(2)),
                        concatenateFields("a", "b"),
                        MatchType.EQUALITY,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2)),
                        keyField("b").groupBy(keyField("a")),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2)),
                        concatenateFields("b", "c").groupBy(keyField("a")),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2)),
                        new GroupingKeyExpression(concat(keyField("a"), keyField("b"), function("nada", concatenateFields("c", "d"))), 1),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2)),
                        keyWithValue(concatenateFields("a", "b"), 1),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2),
                                queryField("c").equalsValue(3)),
                        keyWithValue(concatenateFields("a", "b", "c"), 2),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2)),
                        keyWithValue(concat(keyField("a"), keyField("b"), function("nada", concatenateFields("c", "d"))), 3),
                        MatchType.EQUALITY,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(queryField("c").equalsValue(1)),
                                queryField("b").equalsValue(2)),
                        concat(
                                keyField("p").nest(keyField("c")),
                                keyField("b")),
                        MatchType.EQUALITY,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(queryField("c").equalsValue(1)),
                                queryField("b").equalsValue(2),
                                queryField("a").equalsValue(1)),
                        concat(
                                keyField("p").nest(keyField("c")),
                                keyField("b")),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(queryField("d").equalsValue(1)),
                                queryField("b").equalsValue(2)),
                        concat(
                                keyField("p").nest(keyField("c")),
                                keyField("b")),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        Query.and(
                                queryField("q").matches(queryField("c").equalsValue(1)),
                                queryField("b").equalsValue(2)),
                        concat(
                                keyField("p").nest(keyField("c")),
                                keyField("b")),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(queryField("c").equalsValue(1)),
                                queryField("b").lessThanOrEquals(2)),
                        concat(
                                keyField("p").nest(keyField("c")),
                                keyField("b")),
                        MatchType.INEQUALITY,
                        MatchType.INEQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(queryField("c").lessThanOrEquals(1)),
                                queryField("b").equalsValue(2)),
                        concat(
                                keyField("p").nest(keyField("c")),
                                keyField("b")),
                        MatchType.NO_MATCH,
                        MatchType.NO_MATCH
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(queryField("c1").equalsValue(1)),
                                queryField("p").matches(queryField("c2").equalsValue(2))),
                        concat(
                                keyField("p").nest(keyField("c1")),
                                keyField("p").nest(keyField("c2"))),
                        MatchType.EQUALITY,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2)
                        ),
                        keyField("a"),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                queryField("b").equalsValue(2)
                        ),
                        keyField("b"),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                new RecordTypeKeyComparison("ErsatzRecordType")
                        ),
                        concat(recordType(), keyField("a")),
                        MatchType.EQUALITY,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("a").equalsValue(1),
                                new RecordTypeKeyComparison("ErsatzRecordType")
                        ),
                        recordType(),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2)
                                )),
                                queryField("c").equalsValue(3)
                        ),
                        keyField("p").nest(concatenateFields("a", "b")),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").greaterThan(2)
                                )),
                                queryField("c").equalsValue(3)
                        ),
                        keyField("p").nest(concatenateFields("a", "b")),
                        MatchType.NO_MATCH,
                        MatchType.INEQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").equalsValue(2)
                                )),
                                queryField("c").equalsValue(3)
                        ),
                        concat(
                                keyField("p").nest(
                                        concatenateFields("a", "b")),
                                keyField("c")),
                        MatchType.EQUALITY,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").matches(Query.and(
                                                queryField("c").equalsValue(2),
                                                queryField("d").equalsValue(3)
                                        ))
                                )),
                                queryField("e").equalsValue(4)),
                        concat(
                                keyField("e"),
                                keyField("p").nest("a")),
                        MatchType.NO_MATCH,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(Query.and(
                                        queryField("a").equalsValue(1),
                                        queryField("b").matches(Query.and(
                                                queryField("c").equalsValue(2),
                                                queryField("d").equalsValue(3)
                                        ))
                                )),
                                queryField("e").equalsValue(4)
                        ),
                        concat(
                                keyField("e"),
                                keyField("p").nest(concat(
                                        keyField("b").nest(concatenateFields(
                                                "c", "d")),
                                        keyField("a")))),
                        MatchType.EQUALITY,
                        MatchType.EQUALITY
                ),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(Query.and(
                                        queryField("a").lessThan(1),
                                        queryField("b").matches(Query.and(
                                                queryField("c").equalsValue(2),
                                                queryField("d").equalsValue(3)
                                        ))
                                )),
                                queryField("e").equalsValue(4)
                        ),
                        concat(
                                keyField("e"),
                                keyField("p").nest(concat(
                                        keyField("b").nest(concatenateFields(
                                                "c", "d")),
                                        keyField("a")))),
                        MatchType.INEQUALITY,
                        MatchType.INEQUALITY
                )
        );
    }

    @ParameterizedTest(name = "testQueryAndPatterns[query = {0}, key = {1}]")
    @MethodSource
    void testQueryAndPatterns(QueryComponent query, KeyExpression key, MatchType matchTypeSatisfiesQuery, MatchType matchTypeCoveringKey) {
        assertEquality(matchTypeSatisfiesQuery, query, key);
        assertEqualityCoveringKey(matchTypeCoveringKey, query, key);
    }

    @SuppressWarnings("unused") // used as arguments for parameterized test
    static Stream<Arguments> testTemporarilyNoMatch() {
        return Stream.of(
                Arguments.of(
                        Query.and(queryField("a").equalsValue(3), queryField("b").isEmpty()),
                        concatenateFields("a", "b")),
                Arguments.of(
                        Query.and(
                                queryField("a").lessThan(3),
                                queryField("a").greaterThan(0)
                        ),
                        concatenateFields("a", "b")),
                Arguments.of(
                        Query.not(queryField("a").equalsValue(3)),
                        keyField("a")),
                Arguments.of(
                        Query.or(queryField("a").equalsValue(3), queryField("b").equalsValue(4)),
                        concatenateFields("a", "b")),
                Arguments.of(
                        Query.rank("a").equalsValue(5),
                        keyField("a")),
                Arguments.of(
                        queryField("p").matches(Query.or(queryField("a").equalsValue(3), queryField("b").equalsValue(4))),
                        keyField("p").nest(concatenateFields("a", "b"))),
                Arguments.of(
                        queryField("p").matches(Query.rank("a").equalsValue(5)),
                        keyField("p").nest(keyField("a"))),
                Arguments.of(
                        queryField("p").matches(Query.not(queryField("a").equalsValue(3))),
                        keyField("p").nest(keyField("a"))),
                Arguments.of(
                        queryField("p").matches(Query.and(queryField("a").greaterThan(3),
                                Query.or(queryField("b").lessThan(4), queryField("b").greaterThan(5)))),
                        keyField("p").nest(concatenateFields("a", "b"))),
                Arguments.of(
                        queryField("p").matches(Query.and(
                                queryField("c1").equalsValue(1),
                                queryField("c2").equalsValue(2))),
                        concat(
                                keyField("p").nest(keyField("c1")),
                                keyField("p").nest(keyField("c2")))),
                Arguments.of(
                        Query.and(
                                queryField("p").matches(queryField("c1").equalsValue(1)),
                                queryField("p").matches(queryField("c2").equalsValue(2))),
                        keyField("p").nest(concatenateFields("c1", "c2")))
        );
    }

    /**
     * This is a holder test to make sure we don't forget to test things when we add support for them, and
     * to make sure they return no match for now. Ideally these match correctly once implemented.
     *
     * @param query the query to match to match with
     * @param key the key expression to match with
     */
    @ParameterizedTest(name = "testTemporarilyNoMatch[query = {0}, key = {1}")
    @MethodSource
    void testTemporarilyNoMatch(@Nonnull QueryComponent query, @Nonnull KeyExpression key) {
        assertNoMatch(query, key);
    }

    @SuppressWarnings("unused") // used as arguments for parameterized test
    static Stream<Arguments> testUnexpected() {
        return Stream.of(
                Arguments.of(queryField("a").equalsValue(1), UnknownKeyExpression.UNKNOWN),
                Arguments.of(queryField("a").oneOfThem().equalsValue(1), UnknownKeyExpression.UNKNOWN),
                Arguments.of(
                        queryField("p").matches(queryField("b").equalsValue(1)),
                        UnknownKeyExpression.UNKNOWN),
                Arguments.of(
                        queryField("p").oneOfThem().matches(queryField("b").equalsValue(1)),
                        UnknownKeyExpression.UNKNOWN),
                Arguments.of(
                        queryField("p").matches(queryField("b").equalsValue(1)),
                        keyField("p").nest(UnknownKeyExpression.UNKNOWN)),
                Arguments.of(
                        queryField("p").oneOfThem().matches(queryField("b").equalsValue(1)),
                        keyField("p", FanType.FanOut).nest(UnknownKeyExpression.UNKNOWN)),
                Arguments.of(
                        Query.and(queryField("a").equalsValue(1), queryField("b").equalsValue(2)),
                        concat(keyField("a"), UnknownKeyExpression.UNKNOWN)),
                Arguments.of(
                        new RecordTypeKeyComparison("DummyRecordType"),
                        UnknownKeyExpression.UNKNOWN)
        );
    }

    @ParameterizedTest(name = "testUnexpected[query = {0}, key = {1}]")
    @MethodSource
    void testUnexpected(QueryComponent query, KeyExpression key) {
        // Make sure the places that throw an error when given an unknown expression all do so.
        assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
            final QueryToKeyMatcher matcher = new QueryToKeyMatcher(query);
            matcher.matchesSatisfyingQuery(key);
        });
    }

    private static void assertEquality(MatchType type, QueryComponent query, KeyExpression key) {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(query);
        assertEquals(type, matcher.matchesSatisfyingQuery(key).getType());
    }

    private static void assertEqualityCoveringKey(MatchType type, QueryComponent query, KeyExpression key) {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(query);
        assertEquals(type, matcher.matchesCoveringKey(key).getType());
    }

    private static void assertNoMatch(QueryComponent query, KeyExpression key) {
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(query);
        assertNoMatch(matcher.matchesSatisfyingQuery(key));
    }

    private static void assertNoMatch(Match match) {
        assertEquals(MatchType.NO_MATCH, match.getType());
    }

    private static FieldKeyExpression keyField(String name) {
        return Key.Expressions.field(name);
    }

    private static FieldKeyExpression keyField(String name, FanType fanType) {
        return Key.Expressions.field(name, fanType);
    }

    private static Field queryField(String name) {
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

    private static class DoNothingFunction extends FunctionKeyExpression implements QueryableKeyExpression {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("DoNothing-Function");

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
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
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

        @Nonnull
        @Override
        public Value toValue(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final Type baseType, @Nonnull final List<String> fieldNamePrefix) {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public int planHash(@Nonnull final PlanHashable.PlanHashMode mode) {
            return super.basePlanHash(mode, BASE_HASH);
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return super.baseQueryHash(hashKind, BASE_HASH);
        }
    }
}
