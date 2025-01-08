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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.cache.QueryCacheKey;
import com.apple.foundationdb.relational.recordlayer.util.Hex;
import com.apple.foundationdb.relational.util.Assert;

import com.google.protobuf.ByteString;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Base64;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.foundationdb.relational.recordlayer.query.QueryExecutionContext.OrderedLiteral.constantId;

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
@API(API.Status.EXPERIMENTAL)
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

    @Nonnull
    private static final BitSet emptyBitSet = new BitSet();

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(List.of(query), PreparedParams.empty(), expectedCanonicalRepresentation);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(queries, PreparedParams.empty(), expectedCanonicalRepresentation, queries.stream().map(q -> Map.<String, Object>of()).collect(Collectors.toList()));
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, queries.stream().map(q -> Map.<String, Object>of()).collect(Collectors.toList()));
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final Map<String, Object> expectedParameters) throws RelationalException {
        validate(List.of(query), PreparedParams.empty(), expectedCanonicalRepresentation, List.of(expectedParameters));
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final Map<String, Object> expectedParameters) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation, List.of(expectedParameters));
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParameters) throws RelationalException {
        validate(queries, PreparedParams.empty(), expectedCanonicalRepresentation, expectedParameters, null);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParameters) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, expectedParameters, null);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final Map<String, Object> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(List.of(query), PreparedParams.empty(), expectedCanonicalRepresentation, List.of(expectedParameters), expectedContinuation, -1);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final Map<String, Object> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation, List.of(expectedParameters), expectedContinuation, -1);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(queries, PreparedParams.empty(), expectedCanonicalRepresentation, expectedParameters, expectedContinuation, -1);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParameters,
                                 @Nullable final String expectedContinuation) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, expectedParameters, expectedContinuation, -1);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final Map<String, Object> expectedParameters,
                                 int limit) throws RelationalException {
        validate(List.of(query), PreparedParams.empty(), expectedCanonicalRepresentation, List.of(expectedParameters), null, limit);
    }

    private static void validate(@Nonnull final String query,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final Map<String, Object> expectedParameters,
                                 int limit) throws RelationalException {
        validate(List.of(query), preparedStatementParameters, expectedCanonicalRepresentation, List.of(expectedParameters), null, limit);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParameters,
                                 int limit) throws RelationalException {
        validate(queries, PreparedParams.empty(), expectedCanonicalRepresentation, expectedParameters, null, limit);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedParams preparedStatementParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParameters,
                                 int limit) throws RelationalException {
        validate(queries, preparedStatementParameters, expectedCanonicalRepresentation, expectedParameters, null, limit);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedParams preparedParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParametersList,
                                 @Nullable final String expectedContinuation,
                                 int limit) throws RelationalException {
        validate(queries, preparedParameters, expectedCanonicalRepresentation, expectedParametersList, expectedContinuation, limit, null);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedParams preparedParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParametersList,
                                 @Nullable final String expectedContinuation,
                                 int limit,
                                 @Nullable EnumSet<AstNormalizer.Result.QueryCachingFlags> queryCachingFlags) throws RelationalException {
        validate(queries, preparedParameters, expectedCanonicalRepresentation, expectedParametersList, expectedContinuation, limit, queryCachingFlags, null);
    }

    private static void validate(@Nonnull final List<String> queries,
                                 @Nonnull final PreparedParams preparedParameters,
                                 @Nonnull final String expectedCanonicalRepresentation,
                                 @Nonnull final List<Map<String, Object>> expectedParametersList,
                                 @Nullable final String expectedContinuation,
                                 int limit,
                                 @Nullable EnumSet<AstNormalizer.Result.QueryCachingFlags> queryCachingFlags,
                                 @Nullable Map<Options.Name, Object> queryOptions) throws RelationalException {
        Assert.thatUnchecked(!queries.isEmpty());
        Assert.thatUnchecked(queries.size() == expectedParametersList.size());
        Integer queryHash = null;
        QueryCacheKey queryCacheKey = null;
        for (int i = 0; i < queries.size(); i++) {
            final var query = queries.get(i);
            final var expectedParameters = expectedParametersList.get(i);
            final var hashResults = AstNormalizer.normalizeAst(fakeSchemaTemplate, QueryParser.parse(query).getRootContext(),
                    PreparedParams.copyOf(preparedParameters), 0, emptyBitSet, false, PlanHashable.PlanHashMode.VC0);
            Assertions.assertThat(hashResults.getQueryCacheKey().getCanonicalQueryString()).isEqualTo(expectedCanonicalRepresentation);
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
                final var actualLimit = execParams.getExecutionPropertiesBuilder().getReturnedRowLimit();
                Assertions.assertThat(actualLimit).isEqualTo(limit);
            }
            // verify that all queries share exactly the same hash code.
            if (queryHash == null) {
                queryHash = hashResults.getQueryCacheKey().getHash();
            } else {
                Assertions.assertThat(queryHash).isEqualTo(hashResults.getQueryCacheKey().getHash());
            }
            // verify that all queries are Object.equals == true
            if (queryCacheKey == null) {
                queryCacheKey = hashResults.getQueryCacheKey();
            } else {
                Assertions.assertThat(queryCacheKey).isEqualTo(hashResults.getQueryCacheKey());
            }
            // verify query caching flags, if explicitly expected in the test.
            if (queryCachingFlags != null) {
                Assertions.assertThat(hashResults.getQueryCachingFlags()).isEqualTo(queryCachingFlags);
            }
            if (queryOptions != null) {
                for (final var expectedOption : queryOptions.entrySet()) {
                    final Object actualOption = hashResults.getQueryOptions().getOption(expectedOption.getKey());
                    Assertions.assertThat(actualOption).isNotNull();
                    Assertions.assertThat(actualOption).isEqualTo(expectedOption.getValue());
                }
            }
        }
    }

    private static void shouldFail(@Nonnull final String query, @Nonnull final String errorMessage) {
        try {
            AstNormalizer.normalizeAst(fakeSchemaTemplate, QueryParser.parse(query).getRootContext(),
                    PreparedParams.empty(), 0, emptyBitSet, false, PlanHashable.PlanHashMode.VC0);
            Assertions.fail(String.format("expected %s to fail with %s, but it succeeded!", query, errorMessage));
        } catch (RelationalException | UncheckedRelationalException e) {
            Assertions.assertThat(e.getMessage()).contains(errorMessage);
        }
    }

    private static void validateNotSameHash(@Nonnull final String query1,
                                            @Nonnull final String query2) throws RelationalException {
        validateNotSameHash(query1, query2, PreparedParams.empty());
    }

    private static void validateNotSameHash(@Nonnull final String query1,
                                            @Nonnull final String query2,
                                            @Nonnull PreparedParams preparedParams) throws RelationalException {
        validateNotSameHash(query1, preparedParams, query2, preparedParams);
    }

    private static void validateNotSameHash(@Nonnull final String query1,
                                            @Nonnull PreparedParams preparedParams1,
                                            @Nonnull final String query2,
                                            @Nonnull PreparedParams preparedParams2) throws RelationalException {

        final var result1 = AstNormalizer.normalizeAst(fakeSchemaTemplate, QueryParser.parse(query1).getRootContext(),
                PreparedParams.copyOf(preparedParams1), 0, emptyBitSet, false, PlanHashable.PlanHashMode.VC0);
        final var result2 = AstNormalizer.normalizeAst(fakeSchemaTemplate, QueryParser.parse(query2).getRootContext(),
                PreparedParams.copyOf(preparedParams2), 0, emptyBitSet, false, PlanHashable.PlanHashMode.VC0);
        Assertions.assertThat(result1.getQueryCacheKey().getHash()).isNotEqualTo(result2.getQueryCacheKey().getHash());
    }

    private static void validateNotEqual(@Nonnull final String query1,
                                         @Nonnull final String query2) throws RelationalException {
        validateNotEqual(query1, query2, PreparedParams.empty());
    }

    private static void validateNotEqual(@Nonnull final String query1,
                                         @Nonnull final String query2,
                                         @Nonnull PreparedParams preparedParams) throws RelationalException {
        final var result1 = AstNormalizer.normalizeAst(fakeSchemaTemplate, QueryParser.parse(query1).getRootContext(),
                PreparedParams.copyOf(preparedParams), 0, emptyBitSet, false, PlanHashable.PlanHashMode.VC0);
        final var result2 = AstNormalizer.normalizeAst(fakeSchemaTemplate, QueryParser.parse(query2).getRootContext(),
                PreparedParams.copyOf(preparedParams), 0, emptyBitSet, false, PlanHashable.PlanHashMode.VC0);
        Assertions.assertThat(result1.getQueryCacheKey()).isNotEqualTo(result2.getQueryCacheKey());
    }

    @SuppressWarnings("unchecked")
    private static void compareBindings(@Nonnull final Object actual, @Nonnull final Object expected) {
        Assertions.assertThat(actual instanceof Map).isTrue();
        Assertions.assertThat(expected instanceof Map).isTrue();
        final var actualMap = (Map<String, Object>) actual;
        final var expectedMap = (Map<String, Object>) expected;
        Assertions.assertThat(actualMap.size()).isEqualTo(expectedMap.size());
        for (final var actualEntry : actualMap.entrySet()) {
            final var actualObject = actualEntry.getValue();
            final var expectedObject = expectedMap.get(actualEntry.getKey());
            Assertions.assertThat(expectedObject).as("checking constants %s", actualMap.keySet()).isNotNull();
            Assertions.assertThat(actualObject).as("checking constants %s", actualMap.keySet()).isEqualTo(expectedObject);
        }
    }

    @Nonnull
    private static java.sql.Array toArrayParameter(List<Object> elements) throws SQLException {
        return EmbeddedRelationalArray.newBuilder().addAll(elements.toArray()).build();
    }

    @Test
    void queryHashWorks() throws Exception {
        validate(List.of(
                        "select * from t1 where col1 = col2",
                        "select * from      t1 where col1     = col2",
                        "select * from \n\n\n\t t1 where \n  col1 = col2"),
                "select * from \"T1\" where \"COL1\" = \"COL2\" ");
    }

    @Test
    void queryHashIsCaseSensitive() throws Exception {
        validate("seleCt * fROm t1 whEre col1 = \"cOl2\"",
                "seleCt * fROm \"T1\" whEre \"COL1\" = \"cOl2\" ");
    }

    @Test
    void queryHashingWithParametersWorks() throws RelationalException {
        validate(List.of(
                        "select * from t1 where col1 = 30 and col3 = 90",
                        "select * from      t1 where col1     = 60 and col3 = -4556"),
                "select * from \"T1\" where \"COL1\" = ? and \"COL3\" = ? ",
                List.of(Map.of(constantId(7), 30, constantId(11), 90),
                        Map.of(constantId(7), 60, constantId(11), -4556)));
    }

    @Test
    void queryHashingWithParametersWorksDifferentTypes() throws RelationalException {
        validate(List.of(
                        "select a, 40 from t1 where col1 = 30 and col3 = 90",
                        "select a, 'hello' from      t1 where col1     = 60 and col3 = -4556"),
                "select \"A\" , ? from \"T1\" where \"COL1\" = ? and \"COL3\" = ? ",
                List.of(Map.of(constantId(3), 40, constantId(9), 30, constantId(13), 90),
                        Map.of(constantId(3), "hello", constantId(9), 60, constantId(13), -4556)));
    }

    @Test
    void hashingDoesNotPerformConstantFolding() throws RelationalException {
        validate(List.of(
                        "select 3 + 40 from t1",
                        "select 'hello' + 'world' from      t1"),
                "select ? + ? from \"T1\" ",
                List.of(Map.of(constantId(1), 3, constantId(3), 40),
                        Map.of(constantId(1), "hello", constantId(3), "world")));
    }

    @Test
    void stripBooleanLiteral() throws RelationalException {
        validate("select false, true from t1 where false",
                "select ? , ? from \"T1\" where ? ",
                Map.of(constantId(1), false,
                        constantId(3), true,
                        constantId(7), false));
    }

    @Test
    void stripStringLiteral() throws RelationalException {
        validate("select 'hello', 'wOrLd' from t1 where col1 in ('foo', 'bar')",
                "select ? , ? from \"T1\" where \"COL1\" in ( [ ] ) ",
                Map.of(constantId(1), "hello",
                        constantId(3), "wOrLd",
                        constantId(9), List.of("foo", "bar")));
    }

    @Test
    void stripDecimalLiteral() throws RelationalException {
        validate("select 1, 2.3, 4.5f, -8, -9.1, -2.3f from t1",
                "select ? , ? , ? , ? , ? , ? from \"T1\" ",
                Map.of(constantId(1), 1,
                        constantId(3), 2.3,
                        constantId(5), 4.5f,
                        constantId(7), -8,
                        constantId(10), -9.1,
                        constantId(13), -2.3f));
    }

    @Test
    void stripHexadecimalLiteral() throws RelationalException {
        validate("select X'0A0B' from t1",
                "select ? from \"T1\" ",
                Map.of(constantId(1), ByteString.copyFrom(Hex.decodeHex("0a0b"))));
    }

    @Test
    void stripBase64Literal() throws RelationalException {
        validate("select B64'yv4=' from t1",
                "select ? from \"T1\" ",
                Map.of(constantId(1), ByteString.copyFrom(Hex.decodeHex("cafe"))));
    }

    @Test
    void continuationIsStripped() throws Exception {
        validate(List.of("select * from t1 with continuation b64'yv4='",
                        "select * from   t1 with      continuation x'cafe'",
                        "select * from t1"),
                "select * from \"T1\" ");
    }

    @Test
    void parseContinuation() throws Exception {
        final var expectedContinuationStr = "FBUCFA==";
        validate(List.of("select * from t1 with continuation b64'" + expectedContinuationStr + "'",
                        "select * from t1 with  continuation    b64'" + expectedContinuationStr + "'"),
                PreparedParams.empty(),
                "select * from \"T1\" ",
                List.of(Map.of(), Map.of()),
                expectedContinuationStr,
                -1);
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
                "select * from \"T1\" where \"COL1\" in ( [ ] ) ",
                List.of(Map.of(constantId(7), List.of(10, 100, 1000)),
                        Map.of(constantId(7), List.of(20, 200)),
                        Map.of(constantId(7), List.of(30, 300.0, 3000.1))));
    }

    @Test
    void parseInPredicateSomeConstants() throws Exception {
        // if the in predicate LHS is not composed of simple constants, then we generate a strip
        // the literals and add them _individually_ to the literals array.
        validate("select * from t1 where col1 in (10, col2, 1000)",
                "select * from \"T1\" where \"COL1\" in ( ? , \"COL2\" , ? ) ",
                Map.of(constantId(8), 10,
                        constantId(12), 1000));
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
                "select * from \"T1\" where \"COL1\" in ( ? + ? , ? - ? ) ",
                Map.of(constantId(8), 3,
                        constantId(10), 4,
                        constantId(12), 5,
                        constantId(14), 6));
    }

    @Test
    void nullIsExcludedFromNormalisation() throws Exception {
        validate("select * from t1 where col1 is null",
                "select * from \"T1\" where \"COL1\" is null ");
    }

    @Test
    void isNotNullIsExcludedFromNormalisation() throws Exception {
        validate("select * from t1 where col1 is not null",
                "select * from \"T1\" where \"COL1\" is not null ");
    }

    @Test
    void parseDdlStatementSetsCorrectCachingFlags() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("create schema template aggregate_index_tests_template" +
                        "\n create table t1(id bigint, col1 bigint, col2 bigint, primary key(id))" +
                        "\n create index mv1 as select sum(col2) from t1 where col1 > 42 group by col1"),
                PreparedParams.empty(),
                "create schema template \"AGGREGATE_INDEX_TESTS_TEMPLATE\"" +
                        " create table \"T1\" ( \"ID\" bigint , \"COL1\" bigint , \"COL2\" bigint , primary key ( \"ID\" ) )" +
                        " create index \"MV1\" as select sum ( \"COL2\" ) from \"T1\" where \"COL1\" > ? group by \"COL1\" ",
                List.of(Map.of(constantId(37), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DDL_STATEMENT));
    }

    @Test
    void parseDqlStatementSetsCorrectCachingFlags() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > 42", "  select * from t1   where   col1 > 42"),
                PreparedParams.empty(),
                "select * from \"T1\" where \"COL1\" > ? ",
                List.of(Map.of(constantId(7), 42), Map.of(constantId(7), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT));
    }

    @Test
    void parseDqlStatementWithNoCacheSetsCorrectCachingFlags() throws Exception {
        // (yhatem) note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > 42 options (nocache)", "  select * from t1   where   col1 > 42 options (  nocache    )"),
                PreparedParams.empty(),
                "select * from \"T1\" where \"COL1\" > ? ", // note: the canonical representation is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(7), 42), Map.of(constantId(7), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT,
                        AstNormalizer.Result.QueryCachingFlags.WITH_NO_CACHE_OPTION));
    }

    @Test
    void parseAdministrationStatementCorrectCachingFlags() throws Exception {
        // (yhatem) 'show databases' is _not_ using a string literal in the path prefix, that's why we don't pick it up, we should fix that.
        validate(List.of("show databases with prefix /a/b/c", "  show databases   with prefix \n\n\n /a/b/c\t"),
                PreparedParams.empty(),
                "show databases with prefix \"/A/B/C\" ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(), Map.of()),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_ADMIN_STATEMENT));
    }

    @Test
    void parseUtilityStatementCorrectCachingFlags() throws Exception {
        validate(List.of("explain select * from t1 where col1 > 42", "  explain select  \t * from    t1 \n\n where col1 > 42   \n\n"),
                PreparedParams.empty(),
                "explain select * from \"T1\" where \"COL1\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(8), 42), Map.of(constantId(8), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_UTILITY_STATEMENT));
    }

    @Test
    void parseDqlStatementWithoutLogQuerySetLogQueryFalseFlag() throws Exception {
        // (yhatem) note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > 42", "  select * from t1   where   col1 > 42"),
                PreparedParams.empty(),
                "select * from \"T1\" where \"COL1\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(7), 42), Map.of(constantId(7), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT),
                Map.of(Options.Name.LOG_QUERY, false));
    }

    @Test
    void parseDqlStatementWithLogQuerySetLogQueryFlag() throws Exception {
        // (yhatem) note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > 42 options (log query)", "  select * from t1   where   col1 > 42 options (  log    query)"),
                PreparedParams.empty(),
                "select * from \"T1\" where \"COL1\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(7), 42), Map.of(constantId(7), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT),
                Map.of(Options.Name.LOG_QUERY, true));
    }

    @Test
    void parseDmlStatementWithDryRunSetDryRunFalse() throws Exception {
        validate(List.of("update A set A2 = 52 where A1 > 2"),
                PreparedParams.empty(),
                "update \"A\" set \"A2\" = ? where \"A1\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(5), 52, constantId(9), 2)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DML_STATEMENT),
                Map.of(Options.Name.DRY_RUN, false));
    }

    @Test
    void parseDmlStatementWithDryRunSetDryRunTrue() throws Exception {
        validate(List.of("update A set A2 = 52 where A1 > 2 OPTIONS(DRY RUN)"),
                PreparedParams.empty(),
                "update \"A\" set \"A2\" = ? where \"A1\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(5), 52, constantId(9), 2)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DML_STATEMENT),
                Map.of(Options.Name.DRY_RUN, true));
    }

    @Test
    void parseDmlStatementWithMultipleQueryOptions() throws Exception {
        validate(List.of("update A set A2 = 52 where A1 > 2 OPTIONS(DRY RUN, nocache, log query)"),
                PreparedParams.empty(),
                "update \"A\" set \"A2\" = ? where \"A1\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(5), 52, constantId(9), 2)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DML_STATEMENT, AstNormalizer.Result.QueryCachingFlags.WITH_NO_CACHE_OPTION),
                Map.of(Options.Name.DRY_RUN, true, Options.Name.LOG_QUERY, true));
    }

    @Test
    void queryHashWorksWithPreparedParameters() throws Exception {
        validate(List.of(
                        "select * from t1 where col1 = ? or col2 = ?NamedParam",
                        "select * from      t1 where col1 = ? or col2 = ?NamedParam",
                        "select * from \n\n\n\t t1 where \n  col1 = ? or col2 = ?NamedParam"),
                PreparedParams.of(Map.of(1, 42), Map.of("NamedParam", "foo")),
                "select * from \"T1\" where \"COL1\" = ? or \"COL2\" = ?NamedParam ",
                List.of(Map.of(constantId(7), 42, constantId(11), "foo"),
                        Map.of(constantId(7), 42, constantId(11), "foo"),
                        Map.of(constantId(7), 42, constantId(11), "foo")));
    }

    @Test
    void queryHashingWithParametersWorksWithPreparedParameters() throws RelationalException {
        validate(List.of(
                        "select * from t1 where col1 = 30 and col3 = 90 and col4 = ?",
                        "select * from      t1 where col1     = 60 and col3 = -4556 and    col4 = ?"),
                PreparedParams.ofUnnamed(Map.of(1, 42)),
                "select * from \"T1\" where \"COL1\" = ? and \"COL3\" = ? and \"COL4\" = ? ",
                List.of(Map.of(constantId(7), 30, constantId(11), 90, constantId(15), 42),
                        Map.of(constantId(7), 60, constantId(11), -4556, constantId(16), 42)));
    }

    @Test
    void queryHashingWithParametersWorksDifferentTypesWithPreparedParameters() throws RelationalException {
        validate(List.of(
                        "select a, 40, ? from t1 where col1 = 30 and col3 = 90",
                        "select a, 'hello', ? from      t1 where col1     = 60 and col3 = -4556"),
                PreparedParams.ofUnnamed(Map.of(1, 42)),
                "select \"A\" , ? , ? from \"T1\" where \"COL1\" = ? and \"COL3\" = ? ",
                List.of(Map.of(constantId(3), 40, constantId(5), 42, constantId(11), 30, constantId(15), 90),
                        Map.of(constantId(3), "hello", constantId(5), 42, constantId(11), 60, constantId(15), -4556)));
    }

    @Test
    void hashingDoesNotPerformConstantFoldingWithPreparedParameters() throws RelationalException {
        validate(List.of(
                        "select 3 + 40 + ?NamedParam1 + ?NamedParam2 from t1",
                        "select 'hello' + 'world' + ?NamedParam1 +    ?NamedParam2 from      t1"),
                PreparedParams.ofNamed(Map.of("NamedParam1", 42, "NamedParam2", 100)),
                "select ? + ? + ?NamedParam1 + ?NamedParam2 from \"T1\" ",
                List.of(Map.of(constantId(1), 3, constantId(3), 40, constantId(5), 42, constantId(7), 100),
                        Map.of(constantId(1), "hello", constantId(3), "world", constantId(5), 42, constantId(7), 100)));
    }

    @Test
    void stripBooleanLiteralWithPreparedParameters() throws RelationalException {
        validate("select false, true, ?, ?Param from t1 where false",
                PreparedParams.of(Map.of(1, false), Map.of("Param", true)),
                "select ? , ? , ? , ?Param from \"T1\" where ? ",
                Map.of(constantId(1), false,
                        constantId(3), true,
                        constantId(5), false,
                        constantId(7), true,
                        constantId(11), false));
    }

    @Test
    void stripStringLiteralWithPreparedParameters() throws RelationalException {
        validate("select 'hello', ?, 'wOrLd', ?Param from t1 where col1 in ('foo', 'bar')",
                PreparedParams.of(Map.of(1, "preparedValue1"), Map.of("Param", "preparedValue2")),
                "select ? , ? , ? , ?Param from \"T1\" where \"COL1\" in ( [ ] ) ",
                Map.of(constantId(1), "hello",
                        constantId(3), "preparedValue1",
                        constantId(5), "wOrLd",
                        constantId(7), "preparedValue2",
                        constantId(13), List.of("foo", "bar")));
    }

    @Test
    void stripArrayLiteralWithPreparedParameters() throws RelationalException, SQLException {
        java.sql.Array param = toArrayParameter(List.of("preparedValue1", "preparedValue2"));
        java.sql.Array namedParam = toArrayParameter(List.of("preparedValue3", "preparedValue4"));
        validate("select 'hello', 'wOrLd' from t1 where col1 in ? and col2 in ?param",
                PreparedParams.of(
                        Map.of(1, param),
                        Map.of("param", namedParam)),
                "select ? , ? from \"T1\" where \"COL1\" in ? and \"COL2\" in ?param ",
                Map.of(constantId(1), "hello",
                        constantId(3), "wOrLd",
                        constantId(9), List.of("preparedValue1", "preparedValue2"),
                        constantId(13), List.of("preparedValue3", "preparedValue4")));
    }

    @Test
    void stripDecimalLiteralWithPreparedParameters() throws RelationalException {
        validate("select 1, 2.3, 4.5f, -8, -9.1, -2.3f, ?, ?, ?param1, ?param2 from t1",
                PreparedParams.of(Map.of(1, 1000, 2, -1000), Map.of("param1", 5000, "param2", -5000)),
                "select ? , ? , ? , ? , ? , ? , ? , ? , ?param1 , ?param2 from \"T1\" ",
                Map.of(constantId(1), 1,
                        constantId(3), 2.3,
                        constantId(5), 4.5f,
                        constantId(7), -8,
                        constantId(10), -9.1,
                        constantId(13), -2.3f,
                        constantId(16), 1000,
                        constantId(18), -1000,
                        constantId(20), 5000,
                        constantId(22), -5000));
    }

    @Test
    void stripHexadecimalLiteralWithPreparedParameters() throws RelationalException {
        validate("select X'0A0B', ?, ?param from t1",
                PreparedParams.of(Map.of(1, Hex.decodeHex("0a0c")), Map.of("param", Hex.decodeHex("0B0C"))),
                "select ? , ? , ?param from \"T1\" ",
                Map.of(constantId(1), ByteString.copyFrom(Hex.decodeHex("0A0B")),
                        constantId(3), ByteString.copyFrom(Hex.decodeHex("0A0C")),
                        constantId(5), ByteString.copyFrom(Hex.decodeHex("0B0C"))));
    }

    @Test
    void stripBase64LiteralWithPreparedParameters() throws RelationalException {
        validate("select B64'yv4=', ?, ?param from t1",
                PreparedParams.of(Map.of(1, Hex.decodeHex("0a0c")), Map.of("param", Hex.decodeHex("0B0C"))),
                "select ? , ? , ?param from \"T1\" ",
                Map.of(constantId(1), ByteString.copyFrom(Hex.decodeHex("cafe")),
                        constantId(3), ByteString.copyFrom(Hex.decodeHex("0A0C")),
                        constantId(5), ByteString.copyFrom(Hex.decodeHex("0B0C"))));
    }

    @Test
    void parseContinuationWithPreparedParameters() throws Exception {
        final var expectedContinuationStr = "FBUCFA==";
        final var expectedContinuation = Base64.getDecoder().decode(expectedContinuationStr);
        validate(List.of("select * from t1 with continuation ?",
                        "select * from t1 with  continuation    ?          "),
                PreparedParams.ofUnnamed(Map.of(1, expectedContinuation)),
                "select * from \"T1\" ",
                List.of(Map.of(), Map.of()),
                expectedContinuationStr,
                -1);

        validate(List.of("select * from t1 with continuation ?param",
                        "select * from t1 with  continuation    ?param          "),
                PreparedParams.ofNamed(Map.of("param", expectedContinuation)),
                "select * from \"T1\" ",
                List.of(Map.of(), Map.of()),
                expectedContinuationStr,
                -1);
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
                PreparedParams.of(Map.of(1, "param1"), Map.of("NamedParam", "param2")),
                "select ? , ?NamedParam from \"T1\" where \"COL1\" in ( [ ] ) ",
                List.of(Map.of(constantId(1), "param1",
                                constantId(3), "param2",
                                constantId(9), List.of(10, 100, 1000)),
                        Map.of(constantId(1), "param1",
                                constantId(3), "param2",
                                constantId(9), List.of(20, 200)),
                        Map.of(constantId(1), "param1",
                                constantId(3), "param2",
                                constantId(9), List.of(30, 300.0, 3000.1))));
    }

    @Test
    void parseInPredicateSomeConstantsWithPreparedParameters() throws Exception {
        // if the in predicate LHS is not composed of simple constants, then we generate a strip
        // the literals and add them _individually_ to the literals array.
        validate(List.of("select ?, ?NamedParam1 from t1 where col1 in (10,      ?, ?NamedParam2, ?, 1000)",
                        "select ?, ?NamedParam1 from t1 where col1 in (200,   ?,    ?NamedParam2, ?, 20000)"),
                PreparedParams.of(Map.of(1, "unnamed1", 2, "unnamed2", 3, "unnamed3"),
                        Map.of("NamedParam1", "named1", "NamedParam2", "named2")),
                "select ? , ?NamedParam1 from \"T1\" where \"COL1\" in ( ? , ? , ?NamedParam2 , ? , ? ) ",
                List.of(Map.of(constantId(1), "unnamed1",
                                constantId(3), "named1",
                                constantId(10), 10,
                                constantId(12), "unnamed2",
                                constantId(14), "named2",
                                constantId(16), "unnamed3",
                                constantId(18), 1000),
                        Map.of(constantId(1), "unnamed1",
                                constantId(3), "named1",
                                constantId(10), 200,
                                constantId(12), "unnamed2",
                                constantId(14), "named2",
                                constantId(16), "unnamed3",
                                constantId(18), 20000)));
    }

    @Test
    void parseInPredicateCheckQueriesNotSimilarWithPreparedParameters() throws Exception {
        validateNotSameHash("select * from t1 where col1 in (10, col2, 1000, ?)",
                "select * from t1 where col1 in (10, 100, 1000, ?)", PreparedParams.ofUnnamed(Map.of(1, 42)));
        validateNotEqual("select * from t1 where col1 in (10, col2, 1000)",
                "select * from t1 where col1 in (10, 100, 1000)", PreparedParams.ofUnnamed(Map.of(1, 42)));
    }

    @Test
    void parseDqlStatementWithJavaCallDoesNotCacheFunctionCall() throws Exception {
        validate(List.of("select java_call('a.b.c.Foo', col1, ?, ?namedParam1) From t1"),
                PreparedParams.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "select java_call ( 'a.b.c.Foo' , \"COL1\" , ? , ?namedParam1 ) From \"T1\" ",
                List.of(Map.of(constantId(7), 42, constantId(9), 43)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT));
    }

    @Test
    void parseBitAtomExpressionsWithLiterals() throws Exception {
        validate(List.of("select a & 4 from t"), "select \"A\" & ? from \"T\" ",
                List.of(Map.of(constantId(3), 4)));
        validate(List.of("select a & 2 from t"), "select \"A\" & ? from \"T\" ",
                List.of(Map.of(constantId(3), 2)));
    }

    @Test
    void parseBitAtomExpressionsWithParameters() throws Exception {
        validate("select a & ?m from t",
                PreparedParams.ofNamed(Map.of("m", 4L)),
                "select \"A\" & ?m from \"T\" ", Map.of(constantId(3), 4L));
        validate("select a & ?m from t",
                PreparedParams.ofNamed(Map.of("m", 2L)),
                "select \"A\" & ?m from \"T\" ", Map.of(constantId(3), 2L));
    }

    @Test
    void parseDqlStatementWithJavaCallDifferentFunctions() throws Exception {
        validateNotSameHash("select java_call('a.b.c.Foo', col1) From t1",
                "select java_call('a.b.c.Bar', col1) From t1");
        validateNotEqual("select java_call('a.b.c.Foo', col1) From t1",
                "select java_call('a.b.c.Bar', col1) From t1");
    }

    @Test
    void parseDqlStatementWithJavaCallDifferentParameterlessFunctions() throws Exception {
        validateNotSameHash("select java_call('a.b.c.Foo') From t1",
                "select java_call('a.b.c.Bar') From t1");
        validateNotEqual("select java_call('a.b.c.Foo') From t1",
                "select java_call('a.b.c.Bar') From t1");
    }

    @Test
    void parseDdlStatementSetsCorrectCachingFlagsWithPreparedParameters() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("create schema template aggregate_index_tests_template" +
                        "\n create table t1(id bigint, col1 bigint, col2 bigint, primary key(id))" +
                        "\n create index mv1 as select sum(col2) from t1 where col1 > ?namedParam1 and col2 > ? group by col1"),
                PreparedParams.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "create schema template \"AGGREGATE_INDEX_TESTS_TEMPLATE\"" +
                        " create table \"T1\" ( \"ID\" bigint , \"COL1\" bigint , \"COL2\" bigint , primary key ( \"ID\" ) )" +
                        " create index \"MV1\" as select sum ( \"COL2\" ) from \"T1\" where \"COL1\" > ?namedParam1 and \"COL2\" > ? group by \"COL1\" ",
                List.of(Map.of(constantId(37), 43, constantId(41), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DDL_STATEMENT));
    }

    @Test
    void parseDqlStatementSetsCorrectCachingFlagsWithPreparedParameters() throws Exception {
        // note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > ?namedParam1 and col2 > ?", "  select * from t1   where   col1 > ?namedParam1 and col2 > ?"),
                PreparedParams.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "select * from \"T1\" where \"COL1\" > ?namedParam1 and \"COL2\" > ? ",
                List.of(Map.of(constantId(7), 43, constantId(11), 42),
                        Map.of(constantId(7), 43, constantId(11), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT));
    }

    @Test
    void parseDqlStatementWithNoCacheSetsCorrectCachingFlagsWithPreparedParameters() throws Exception {
        // (yhatem) note that the materialised view definition does not set IS_DQL_STATEMENT flag.
        validate(List.of("select * from t1 where col1 > ?namedParam1 and col2 > ? options (nocache)", "  select * from t1   where   col1 > ?namedParam1 and col2 > ? options (  nocache    )"),
                PreparedParams.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "select * from \"T1\" where \"COL1\" > ?namedParam1 and \"COL2\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(7), 43, constantId(11), 42),
                        Map.of(constantId(7), 43, constantId(11), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT,
                        AstNormalizer.Result.QueryCachingFlags.WITH_NO_CACHE_OPTION));
    }

    @Test
    void parseDqlStatementWithoutLogQuerySetsOptionsLogQueryFalseWithPreparedParameters() throws Exception {
        validate(List.of("select * from t1 where col1 > ?namedParam1 and col2 > ?", "  select * from t1   where   col1 > ?namedParam1 and col2 > ?"),
                PreparedParams.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "select * from \"T1\" where \"COL1\" > ?namedParam1 and \"COL2\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(7), 43, constantId(11), 42),
                        Map.of(constantId(7), 43, constantId(11), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT),
                Map.of(Options.Name.LOG_QUERY, false));
    }

    @Test
    void parseDqlStatementWithLogQuerySetsOptionsLogQueryTrueWithPreparedParameters() throws Exception {
        validate(List.of("select * from t1 where col1 > ?namedParam1 and col2 > ? options (log query)", "  select * from t1   where   col1 > ?namedParam1 and col2 > ? options (  log       query)"),
                PreparedParams.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "select * from \"T1\" where \"COL1\" > ?namedParam1 and \"COL2\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(7), 43, constantId(11), 42),
                        Map.of(constantId(7), 43, constantId(11), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_DQL_STATEMENT),
                Map.of(Options.Name.LOG_QUERY, true));
    }

    // (yhatem) we do not have administration statements that can be prepared with parameters.
    // I think we can write a test using `show databases` once we fix its path prefix to become a normal string literal.

    @Test
    void parseUtilityStatementCorrectCachingFlagsWithPreparedParameters() throws Exception {
        validate(List.of("explain select * from t1 where col1 > ?namedParam1 and col2   > ?", "  explain select  \t * from    t1 \n\n where col1 > ?namedParam1 and   col2 > ?   \n\n"),
                PreparedParams.of(Map.of(1, 42), Map.of("namedParam1", 43)),
                "explain select * from \"T1\" where \"COL1\" > ?namedParam1 and \"COL2\" > ? ", // note: this is irrelevant as the query will be recompiled anyway.
                List.of(Map.of(constantId(8), 43, constantId(12), 42),
                        Map.of(constantId(8), 43, constantId(12), 42)),
                null,
                -1,
                EnumSet.of(AstNormalizer.Result.QueryCachingFlags.IS_UTILITY_STATEMENT));
    }

    @Test
    void hashSyntacticallyIncorrectQueryFails() {
        shouldFail("selec * from t1", "syntax error");
    }
}
