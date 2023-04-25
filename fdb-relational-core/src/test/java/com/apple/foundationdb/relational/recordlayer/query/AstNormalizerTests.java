/*
 * QueryHashingTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.cache.CachedQuery;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This tests different aspects of quick AST hashing of {@link AstNormalizer}. Namely:
 * <ul>
 *     <li>canonical equivalence of queries with minor lexical differences.</li>
 *     <li>literal stripping</li>
 *     <li>integration of prepared parameters and stripped literals</li>
 *     <li>processing of execution parameters (limit and continuation)</li>
 *     <li>identifying query caching flags correctly</li>
 *     <li>documentation of expected failures, e.g. hashing syntactically incorrect query</li>
 *     <li>documentation of expected behaviors, e.g. not doing constant folding</li>
 *     <li>documentation of handling IN predicate with all-const in-list and  some-const in-list</li>
 * </ul>
 */
public class AstNormalizerTests {

    @Nonnull
    private static final SchemaTemplate fakeSchemaTemplate = RecordLayerSchemaTemplate
            .newBuilder()
            .setName("testTemplate")
            .addTable(RecordLayerTable
                    .newBuilder(false)
                    .setName("testTable")
                    .addColumn(RecordLayerColumn
                            .newBuilder()
                            .setName("testColumn")
                            .setDataType(DataType.Primitives.BOOLEAN.type())
                            .build())
                    .build())
            .build();

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(List.of(query), PreparedStatementParameters.empty(), expectedCanonicalRepresentation);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(queries, PreparedStatementParameters.empty(), expectedCanonicalRepresentation, queries.stream().map(q -> List.of()).collect(Collectors.toList()));
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, queries.stream().map(q -> List.of()).collect(Collectors.toList()));
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Object> expectedParameters) throws RelationalException {
        validate(List.of(query), PreparedStatementParameters.empty(), expectedCanonicalRepresentation, List.of(expectedParameters));
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Object> expectedParameters) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation, List.of(expectedParameters));
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParameters) throws RelationalException {
        validate(queries, PreparedStatementParameters.empty(), expectedCanonicalRepresentation, expectedParameters, null);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParameters) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, expectedParameters, null);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Object> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(List.of(query), PreparedStatementParameters.empty(), expectedCanonicalRepresentation, List.of(expectedParameters), expectedContinuation, -1);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Object> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation, List.of(expectedParameters), expectedContinuation, -1);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(queries, PreparedStatementParameters.empty(), expectedCanonicalRepresentation, expectedParameters, expectedContinuation, -1);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, expectedParameters, expectedContinuation, -1);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Object> expectedParameters,
                                 int limit) throws RelationalException {
        validate(List.of(query), PreparedStatementParameters.empty(), expectedCanonicalRepresentation, List.of(expectedParameters), null, limit);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Object> expectedParameters,
                                 int limit) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation, List.of(expectedParameters), null, limit);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParameters,
                                 int limit) throws RelationalException {
        validate(queries, PreparedStatementParameters.empty(), expectedCanonicalRepresentation, expectedParameters, null, limit);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParameters,
                                 int limit) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, expectedParameters, null, limit);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParametersList,
                                 @Nullable final String expectedContinuation,
                                 int limit) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, expectedParametersList, expectedContinuation, limit, null);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedStatementParameters preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<List<Object>> expectedParametersList,
                                 @Nullable final String expectedContinuation,
                                 int limit,
                                 @Nullable EnumSet<AstNormalizer.Result.QueryCachingFlags> queryCachingFlags) throws RelationalException {
        Assert.thatUnchecked(!queries.isEmpty());
        Assert.thatUnchecked(queries.size() == expectedParametersList.size());
        Integer queryHash = null;
        CachedQuery cachedQuery = null;
        for (int i = 0; i < queries.size(); i++) {
            final var query = queries.get(i);
            final var expectedParameters = expectedParametersList.get(i);
            final var hashResults = AstNormalizer.normalizeQuery(fakeSchemaTemplate, query, PreparedStatementParameters.of(preparedStatementParameters));
            Assertions.assertThat(hashResults.getCachedQuery().getCanonicalQueryString()).isEqualTo(expectedCanonicalRepresentation);
            final var execParams = hashResults.getQueryExecutionParameters();
            final var evaluationContext = execParams.getEvaluationContext();
            final var constantBindingName = Bindings.Internal.CONSTANT.bindingName(Quantifier.constant().getId());
            if (evaluationContext.getBindings().containsBinding(constantBindingName)) {
                final var binding = evaluationContext.getBinding(constantBindingName);
                compareBindings(binding, expectedParameters);
            } else {
                if (!expectedParameters.isEmpty()) {
                    Assertions.fail(String.format("expected '%s' parameters, actual parameters is however empty", expectedParameters));
                }
            }
            if (expectedContinuation != null) {
                Assertions.assertThat(Base64.getEncoder().encodeToString(execParams.getContinuation())).isEqualTo(expectedContinuation);
            }
            if (limit != -1) {
                final var actualLimit = execParams.getExecutionProperties(execParams.getEvaluationContext()).getReturnedRowLimit();
                Assertions.assertThat(actualLimit).isEqualTo(limit);
            }
            // verify that all queries share exactly the same hash code.
            if (queryHash == null) {
                queryHash = hashResults.getCachedQuery().getHash();
            } else {
                Assertions.assertThat(queryHash).isEqualTo(hashResults.getCachedQuery().getHash());
            }
            // verify that all queries are Object.equals == true
            if (cachedQuery == null) {
                cachedQuery = hashResults.getCachedQuery();
            } else {
                Assertions.assertThat(cachedQuery).isEqualTo(hashResults.getCachedQuery());
            }
            // verify query caching flags, if explicitly expected in the test.
            if (queryCachingFlags != null) {
                Assertions.assertThat(hashResults.getQueryCachingFlags()).isEqualTo(queryCachingFlags);
            }
        }
    }

    private static void shouldFail(@Nonnull final String query, @Nonnull final String errorMessage) {
        try {
            AstNormalizer.normalizeQuery(fakeSchemaTemplate, query);
            Assertions.fail(String.format("expected %s to fail with %s, but it succeeded!", query, errorMessage));
        } catch (RelationalException | UncheckedRelationalException e) {
            Assertions.assertThat(e.getMessage()).contains(errorMessage);
        }
    }

    private static void validateNotSameHash(@Nonnull final String query1,
                                            @Nonnull final String query2) throws RelationalException {
        validateNotSameHash(query1, query2, PreparedStatementParameters.empty());
    }

    private static void validateNotSameHash(@Nonnull final String query1,
                                            @Nonnull final String query2,
                                            @Nonnull PreparedStatementParameters preparedParams) throws RelationalException {
        final var result1 = AstNormalizer.normalizeQuery(fakeSchemaTemplate, query1, PreparedStatementParameters.of(preparedParams));
        final var result2 = AstNormalizer.normalizeQuery(fakeSchemaTemplate, query2, PreparedStatementParameters.of(preparedParams));
        Assertions.assertThat(result1.getCachedQuery().getHash()).isNotEqualTo(result2.getCachedQuery().getHash());
    }

    private static void validateNotEqual(@Nonnull final String query1,
                                         @Nonnull final String query2) throws RelationalException {
        validateNotEqual(query1, query2, PreparedStatementParameters.empty());
    }

    private static void validateNotEqual(@Nonnull final String query1,
                                         @Nonnull final String query2,
                                         @Nonnull PreparedStatementParameters preparedParams) throws RelationalException {
        final var result1 = AstNormalizer.normalizeQuery(fakeSchemaTemplate, query1, PreparedStatementParameters.of(preparedParams));
        final var result2 = AstNormalizer.normalizeQuery(fakeSchemaTemplate, query2, PreparedStatementParameters.of(preparedParams));
        Assertions.assertThat(result1.getCachedQuery()).isNotEqualTo(result2.getCachedQuery());
    }

    private static void compareBindings(@Nonnull final Object actual, @Nonnull final Object expected) {
        Assertions.assertThat(actual instanceof List).isTrue();
        Assertions.assertThat(expected instanceof List).isTrue();
        final List<Object> actualList = (List<Object>) actual;
        final List<Object> expectedList = (List<Object>) expected;
        Assertions.assertThat(actualList.size()).isEqualTo(expectedList.size());
        for (int i = 0; i < actualList.size(); i++) {
            final var actualObject = actualList.get(i);
            final var expectedObject = expectedList.get(i);
            if (actualObject.getClass().isArray()) {
                Assertions.assertThat(expectedObject.getClass().isArray()).isTrue();
                Assertions.assertThat(toObjectArray(actualObject)).isEqualTo(toObjectArray(expectedObject));
            } else {
                Assertions.assertThat(actualObject).isEqualTo(expectedObject);
            }
        }
    }

    @Nonnull
    private static Object[] toObjectArray(@Nonnull final Object val) {
        if (val instanceof Object[]) {
            return (Object[]) val;
        }
        int length = Array.getLength(val);
        Object[] outputArray = new Object[length];
        for (int i = 0; i < length; i++) {
            outputArray[i] = Array.get(val, i);
        }
        return outputArray;
    }

    @Test
    void queryHashWorks() throws Exception {
        validate(List.of(
                        "select * from t1 where col1 = col2",
                        "select * from      t1 where col1     = col2",
                        "select * from \n\n\n\t t1 where \n  col1 = col2"),
                "select * from t1 where col1 = col2 ");
    }

    @Test
    void queryHashIsCaseSensitive() throws Exception {
        validate("seleCt * fROm t1 whEre col1 = cOl2",
                "seleCt * fROm t1 whEre col1 = cOl2 ");
    }

    @Test
    void queryHashingWithParametersWorks() throws RelationalException {
        validate(List.of(
                        "select * from t1 where col1 = 30 and col3 = 90",
                        "select * from      t1 where col1     = 60 and col3 = -4556"),
                "select * from t1 where col1 = ? and col3 = ? ",
                List.of(List.of(30, 90),
                        List.of(60, -4556)));
    }

    @Test
    void queryHashingWithParametersWorksDifferentTypes() throws RelationalException {
        validate(List.of(
                        "select a, 40 from t1 where col1 = 30 and col3 = 90",
                        "select a, 'hello' from      t1 where col1     = 60 and col3 = -4556"),
                "select a , ? from t1 where col1 = ? and col3 = ? ",
                List.of(List.of(40, 30, 90),
                        List.of("hello", 60, -4556)));
    }

    @Test
    void hashingDoesNotPerformConstantFolding() throws RelationalException {
        validate(List.of(
                        "select 3 + 40 from t1",
                        "select 'hello' + 'world' from      t1"),
                "select ? + ? from t1 ",
                List.of(List.of(3, 40),
                        List.of("hello", "world")));
    }

    @Test
    void stripBooleanLiteral() throws RelationalException {
        validate("select false, true from t1 where false",
                "select ? , ? from t1 where ? ",
                List.of(false, true, false));
    }

    @Test
    void stripStringLiteral() throws RelationalException {
        validate("select 'hello', 'wOrLd' from t1 where col1 in ('foo', 'bar')",
                "select ? , ? from t1 where col1 in ( [ ] ) ",
                List.of("hello", "wOrLd", List.of("foo", "bar")));
    }

    @Test
    void stripDecimalLiteral() throws RelationalException {
        validate("select 1, 2.3, 4.5f, -8, -9.1, -2.3f from t1",
                "select ? , ? , ? , ? , ? , ? from t1 ",
                List.of(1, 2.3, 4.5f, -8, -9.1, -2.3f));
    }

    @Test
    void stripHexadecimalLiteral() throws RelationalException {
        validate("select X'0A0B' from t1",
                "select ? from t1 ",
                List.of(new BigInteger("0A0B", 16).longValue()));
    }

    @Test
    void parseLimit() throws Exception {
        validate(List.of("select * from t1 limit 100",
                        "select * from t1 limit             100   "),
                "select * from t1 ",
                List.of(List.of(), List.of()),
                100);
    }

    @Test
    void limitIsStripped() throws Exception {
        validate(List.of("select * from t1 limit 100",
                        "select * from   t1 limit     200",
                        "select * from   t1 lIMIt     200",
                        "select * from t1"),
                "select * from t1 ");
    }

    @Test
    void continuationIsStripped() throws Exception {
        validate(List.of("select * from t1 with continuation 'foo'",
                        "select * from   t1 with      continuation 'foo'",
                        "select * from   t1 lIMIt     200   with conTINUation 'bar'",
                        "select * from t1"),
                "select * from t1 ");
    }

    @Test
    void parseContinuation() throws Exception {
        final var expectedContinuationStr = "FBUCFA==";
        validate(List.of("select * from t1 limit 100 with continuation '" + expectedContinuationStr + "'",
                        "select * from t1 limit             100   with  continuation    '" + expectedContinuationStr + "'"),
                PreparedStatementParameters.empty(),
                "select * from t1 ",
                List.of(List.of(), List.of()),
                expectedContinuationStr,
                100);
    }

    @Test
    void parseInPredicateAllConstants() throws Exception {
        // although these queries have different number of arguments in their in-predicate
        // they are treated as equivalent because Cascades will plan them the same way
        // since the LHS of the in-predicate is composed of simple constants.
        // moreover, the binding will contain _one_ element which is an array of _all_ constants.
        validate(List.of("select * from t1 where col1 in (10, 100, 1000)",
                        "select * from t1 where col1 in (20,   200)",
                        "select * from t1 where col1 in (30,   300.0,    3000.1)"),
                "select * from t1 where col1 in ( [ ] ) ",
                List.of(List.of(List.of(10, 100, 1000)),
                        List.of(List.of(20, 200)),
                        List.of(List.of(30, 300.0, 3000.1))));
    }

    @Test
    void parseInPredicateSomeConstants() throws Exception {
        // if the in predicate LHS is not composed of simple constants, then we generate a strip
        // the literals and add them _individually_ to the literals array.
        validate("select * from t1 where col1 in (10, col2, 1000)",
                "select * from t1 where col1 in ( ? , col2 , ? ) ",
                List.of(10, 1000));
    }

    @Test
    void parseInPredicateCheckQueriesNotSimilar() throws Exception {
        validateNotSameHash("select * from t1 where col1 in (10, col2, 1000)",
                "select * from t1 where col1 in (10, 100, 1000)");
        validateNotEqual("select * from t1 where col1 in (10, col2, 1000)",
                "select * from t1 where col1 in (10, 100, 1000)");
    }

    @Test
    void parseInPredicateWithConstantExpressionsNoConstantFolding() throws Exception {
        // no constant folding, hashes are different
        validateNotSameHash("select * from t1 where col1 in ( 1 + 1 )",
                "select * from t1 where col1 in ( 2 )");
        validateNotEqual("select * from t1 where col1 in ( 1 + 1 )",
                "select * from t1 where col1 in ( 2 )");
    }

    @Test
    void parseInPredicateWithConstantExpressions() throws Exception {
        validate("select * from t1 where col1 in ( 3 + 4 , 5 - 6 )",
                "select * from t1 where col1 in ( ? + ? , ? - ? ) ",
                List.of(3, 4, 5, 6));
    }

    @Test
    void nullIsExcludedFromNormalisation() throws Exception {
        validate("select * from t1 where col1 is null",
                "select * from t1 where col1 is null ");
    }

    @Test
    void isNotNullIsExcludedFromNormalisation() throws Exception {
        validate("select * from t1 where col1 is not null",
                "select * from t1 where col1 is not null ");
    }

    @Test
    void parseDdlStatementSetsCorrectCachingFlags() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("create schema template aggregate_index_tests_template" +
                        "\n create table t1(id bigint, col1 bigint, col2 bigint, primary key(id))" +
                        "\n create index mv1 as select sum(col2) from t1 where col1 > 42 group by col1"),
                PreparedStatementParameters.empty(),
                "create schema template aggregate_index_tests_template" +
                        " create table t1 ( id bigint , col1 bigint , col2 bigint , primary key ( id ) )" +
                        " create index mv1 as select sum ( col2 ) from t1 where col1 > ? group by col1 ",
                List.of(List.of(42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DDL_STATEMENT));
    }

    @Test
    void parseDqlStatementSetsCorrectCachingFlags() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > 42", "  select * from t1   where   col1 > 42"),
                PreparedStatementParameters.empty(),
                "select * from t1 where col1 > ? ",
                List.of(List.of(42), List.of(42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT));
    }

    @Test
    void parseDqlStatementWithNoCacheSetsCorrectCachingFlags() throws Exception {
        // (yhatem) note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > 42 options (nocache)", "  select * from t1   where   col1 > 42 options (  nocache    )"),
                PreparedStatementParameters.empty(),
                "select * from t1 where col1 > ? options ( nocache ) ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(List.of(42), List.of(42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT,
                        AstNormalizer.Result.QueryCachingFlags.WITH_NO_CACHE_OPTION));
    }

    @Test
    void parseAdministrationStatementCorrectCachingFlags() throws Exception {
        // (yhatem) 'show databases' is _not_ using a string literal in the path prefix, that's why we don't pick it up, we should fix that.
        validate(List.of("show databases with prefix /a/b/c", "  show databases   with prefix \n\n\n /a/b/c\t"),
                PreparedStatementParameters.empty(),
                "show databases with prefix /a/b/c ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(List.of(), List.of()),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_ADMIN_STATEMENT));
    }

    @Test
    void parseUtilityStatementCorrectCachingFlags() throws Exception {
        validate(List.of("explain select * from t1 where col1 > 42", "  explain select  \t * from    t1 \n\n where col1 > 42   \n\n"),
                PreparedStatementParameters.empty(),
                "explain select * from t1 where col1 > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(List.of(42), List.of(42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_UTILITY_STATEMENT));
    }

    @Test
    void queryHashWorksWithPreparedParameters() throws Exception {
        validate(List.of(
                        "select * from t1 where col1 = ? or col2 = ?NamedParam",
                        "select * from      t1 where col1 = ? or col2 = ?NamedParam",
                        "select * from \n\n\n\t t1 where \n  col1 = ? or col2 = ?NamedParam"),
                PreparedStatementParameters.of(Map.of(1, 42), Map.of("NamedParam", "foo")),
                "select * from t1 where col1 = ? or col2 = ?NamedParam ",
                List.of(List.of(42, "foo"), List.of(42, "foo"), List.of(42, "foo")));
    }

    @Test
    void queryHashingWithParametersWorksWithPreparedParameters() throws RelationalException {
        validate(List.of(
                        "select * from t1 where col1 = 30 and col3 = 90 and col4 = ?",
                        "select * from      t1 where col1     = 60 and col3 = -4556 and    col4 = ?"),
                PreparedStatementParameters.ofUnnamed(Map.of(1, 42)),
                "select * from t1 where col1 = ? and col3 = ? and col4 = ? ",
                List.of(List.of(30, 90, 42),
                        List.of(60, -4556, 42)));
    }

    @Test
    void queryHashingWithParametersWorksDifferentTypesWithPreparedParameters() throws RelationalException {
        validate(List.of(
                        "select a, 40, ? from t1 where col1 = 30 and col3 = 90",
                        "select a, 'hello', ? from      t1 where col1     = 60 and col3 = -4556"),
                PreparedStatementParameters.ofUnnamed(Map.of(1, 42)),
                "select a , ? , ? from t1 where col1 = ? and col3 = ? ",
                List.of(List.of(40, 42, 30, 90),
                        List.of("hello", 42, 60, -4556)));
    }

    @Test
    void hashingDoesNotPerformConstantFoldingWithPreparedParameters() throws RelationalException {
        validate(List.of(
                        "select 3 + 40 + ?NamedParam1 + ?NamedParam2 from t1",
                        "select 'hello' + 'world' + ?NamedParam1 +    ?NamedParam2 from      t1"),
                PreparedStatementParameters.ofNamed(Map.of("NamedParam1", 42, "NamedParam2", 100)),
                "select ? + ? + ?NamedParam1 + ?NamedParam2 from t1 ",
                List.of(List.of(3, 40, 42, 100),
                        List.of("hello", "world", 42, 100)));
    }

    @Test
    void stripBooleanLiteralWithPreparedParameters() throws RelationalException {
        validate("select false, true, ?, ?Param from t1 where false",
                PreparedStatementParameters.of(Map.of(1, false), Map.of("Param", true)),
                "select ? , ? , ? , ?Param from t1 where ? ",
                List.of(false, true, false, true, false));
    }

    @Test
    void stripStringLiteralWithPreparedParameters() throws RelationalException {
        validate("select 'hello', ?, 'wOrLd', ?Param from t1 where col1 in ('foo', 'bar')",
                PreparedStatementParameters.of(Map.of(1, "preparedValue1"), Map.of("Param", "preparedValue2")),
                "select ? , ? , ? , ?Param from t1 where col1 in ( [ ] ) ",
                List.of("hello", "preparedValue1", "wOrLd", "preparedValue2", List.of("foo", "bar")));
    }

    @Test
    void stripDecimalLiteralWithPreparedParameters() throws RelationalException {
        validate("select 1, 2.3, 4.5f, -8, -9.1, -2.3f, ?, ?, ?param1, ?param2 from t1",
                PreparedStatementParameters.of(Map.of(1, 1000, 2, -1000), Map.of("param1", 5000, "param2", -5000)),
                "select ? , ? , ? , ? , ? , ? , ? , ? , ?param1 , ?param2 from t1 ",
                List.of(1, 2.3, 4.5f, -8, -9.1, -2.3f, 1000, -1000, 5000, -5000));
    }

    @Test
    void stripHexadecimalLiteralWithPreparedParameters() throws RelationalException {
        validate("select X'0A0B', ?, ?param from t1",
                PreparedStatementParameters.of(Map.of(1, new BigInteger("0A0C", 16)), Map.of("param", new BigInteger("0B0C", 16))),
                "select ? , ? , ?param from t1 ",
                List.of(new BigInteger("0A0B", 16).longValue(), new BigInteger("0A0C", 16), new BigInteger("0B0C", 16)));
    }

    @Test
    void parseLimitWithPreparedParameters() throws Exception {
        validate(List.of("select * from t1 limit ?",
                        "select * from t1 limit             ?   "),
                PreparedStatementParameters.ofUnnamed(Map.of(1, 100)),
                "select * from t1 ",
                List.of(List.of(), List.of()),
                100);

        validate(List.of("select * from t1 limit ?param",
                        "select * from t1 limit             ?param   "),
                PreparedStatementParameters.ofNamed(Map.of("param", 100)),
                "select * from t1 ",
                List.of(List.of(), List.of()),
                100);
    }

    @Test
    void parseContinuationWithPreparedParameters() throws Exception {
        final var expectedContinuationStr = "FBUCFA==";
        final var expectedContinuation = Base64.getDecoder().decode(expectedContinuationStr);
        validate(List.of("select * from t1 limit 100 with continuation ?",
                        "select * from t1 limit             100   with  continuation    ?          "),
                PreparedStatementParameters.ofUnnamed(Map.of(1, expectedContinuation)),
                "select * from t1 ",
                List.of(List.of(), List.of()),
                expectedContinuationStr,
                100);

        validate(List.of("select * from t1 limit 100 with continuation ?param",
                        "select * from t1 limit             100   with  continuation    ?param          "),
                PreparedStatementParameters.ofNamed(Map.of("param", expectedContinuation)),
                "select * from t1 ",
                List.of(List.of(), List.of()),
                expectedContinuationStr,
                100);
    }

    @Test
    void parseInPredicateAllConstantsWithPreparedParameters() throws Exception {
        // although these queries have different number of arguments in their in-predicate
        // they are treated as equivalent because Cascades will plan them the same way
        // since the LHS of the in-predicate is composed of simple constants.
        // moreover, the binding will contain _one_ element which is an array of _all_ constants.
        validate(List.of("select ?, ?NamedParam from t1 where col1 in (10, 100, 1000)",
                        "select ?, ?NamedParam from t1 where col1 in (20,   200)",
                        "select ?, ?NamedParam from t1 where col1 in (30,   300.0,    3000.1)"),
                PreparedStatementParameters.of(Map.of(1, "param1"), Map.of("NamedParam", "param2")),
                "select ? , ?NamedParam from t1 where col1 in ( [ ] ) ",
                List.of(List.of("param1", "param2", List.of(10, 100, 1000)),
                        List.of("param1", "param2", List.of(20, 200)),
                        List.of("param1", "param2", List.of(30, 300.0, 3000.1))));
    }

    @Test
    void parseInPredicateSomeConstantsWithPreparedParameters() throws Exception {
        // if the in predicate LHS is not composed of simple constants, then we generate a strip
        // the literals and add them _individually_ to the literals array.
        validate(List.of("select ?, ?NamedParam1 from t1 where col1 in (10,      ?, ?NamedParam2, ?, 1000)",
                        "select ?, ?NamedParam1 from t1 where col1 in (200,   ?,    ?NamedParam2, ?, 20000)"),
                PreparedStatementParameters.of(Map.of(1, "unnamed1", 2, "unnamed2", 3, "unnamed3"),
                        Map.of("NamedParam1", "named1", "NamedParam2", "named2")),
                "select ? , ?NamedParam1 from t1 where col1 in ( ? , ? , ?NamedParam2 , ? , ? ) ",
                List.of(List.of("unnamed1", "named1", 10, "unnamed2", "named2", "unnamed3", 1000),
                        List.of("unnamed1", "named1", 200, "unnamed2", "named2", "unnamed3", 20000)));
    }

    @Test
    void parseInPredicateCheckQueriesNotSimilarWithPreparedParameters() throws Exception {
        validateNotSameHash("select * from t1 where col1 in (10, col2, 1000, ?)",
                "select * from t1 where col1 in (10, 100, 1000, ?)", PreparedStatementParameters.ofUnnamed(Map.of(1, 42)));
        validateNotEqual("select * from t1 where col1 in (10, col2, 1000)",
                "select * from t1 where col1 in (10, 100, 1000)",  PreparedStatementParameters.ofUnnamed(Map.of(1, 42)));
    }

    @Test
    void parseDdlStatementSetsCorrectCachingFlagsWithPreparedParameters() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("create schema template aggregate_index_tests_template" +
                        "\n create table t1(id bigint, col1 bigint, col2 bigint, primary key(id))" +
                        "\n create index mv1 as select sum(col2) from t1 where col1 > ?namedParam1 and col2 > ? group by col1"),
                PreparedStatementParameters.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "create schema template aggregate_index_tests_template" +
                        " create table t1 ( id bigint , col1 bigint , col2 bigint , primary key ( id ) )" +
                        " create index mv1 as select sum ( col2 ) from t1 where col1 > ?namedParam1 and col2 > ? group by col1 ",
                List.of(List.of(43, 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DDL_STATEMENT));
    }

    @Test
    void parseDqlStatementSetsCorrectCachingFlagsWithPreparedParameters() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > ?namedParam1 and col2 > ?", "  select * from t1   where   col1 > ?namedParam1 and col2 > ?"),
                PreparedStatementParameters.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "select * from t1 where col1 > ?namedParam1 and col2 > ? ",
                List.of(List.of(43, 42), List.of(43, 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT));
    }

    @Test
    void parseDqlStatementWithNoCacheSetsCorrectCachingFlagsWithPreparedParameters() throws Exception {
        // (yhatem) note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > ?namedParam1 and col2 > ? options (nocache)", "  select * from t1   where   col1 > ?namedParam1 and col2 > ? options (  nocache    )"),
                PreparedStatementParameters.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "select * from t1 where col1 > ?namedParam1 and col2 > ? options ( nocache ) ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(List.of(43, 42), List.of(43, 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT,
                        AstNormalizer.Result.QueryCachingFlags.WITH_NO_CACHE_OPTION));
    }

    // (yhatem) we do not have administration statements that can be prepared with parameters.
    // I think we can write a test using `show databases` once we fix its path prefix to become a normal string literal.

    @Test
    void parseUtilityStatementCorrectCachingFlagsWithPreparedParameters() throws Exception {
        validate(List.of("explain select * from t1 where col1 > ?namedParam1 and col2   > ?", "  explain select  \t * from    t1 \n\n where col1 > ?namedParam1 and   col2 > ?   \n\n"),
                PreparedStatementParameters.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "explain select * from t1 where col1 > ?namedParam1 and col2 > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(List.of(43, 42), List.of(43, 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_UTILITY_STATEMENT));
    }

    @Test
    void hashSyntacticallyIncorrectQueryFails() {
        shouldFail("selec * from t1", "syntax error");
    }

    @Test
    void hashQueryWithMultipleLimitsFails() {
        shouldFail("select * from (select * from t1 limit 100) a, (select * from t2 limit 200) b",
                "setting multiple limits is not supported");
    }
}
