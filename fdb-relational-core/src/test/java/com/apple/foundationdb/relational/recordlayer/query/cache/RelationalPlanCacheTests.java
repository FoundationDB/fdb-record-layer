/*
 * RelationalPlanCacheTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.EvaluatesToValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OfTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PlannerConfiguration;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.google.common.collect.ImmutableList;
import com.google.common.testing.FakeTicker;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.relational.recordlayer.query.OrderedLiteral.constantId;

/**
 * This contains a set of tests for {@link RelationalPlanCache} and {@link PlanGenerator}. Similar to {@link MultiStageCacheTests},
 * it verifies different aspects of cache behaviour such as primary key and secondary key generation and segregation and
 * different cache eviction policies.
 * <br>
 * However, it runs tests using {@link RelationalPlanCache} specialization, including all SQL processing pipeline components
 * such as SQL normalizer, query hasher, literal extraction, plan generation, plan optimization, plan costing, and cache
 * value reduction operations. The only difference between this test and a production setup is that the cache uses a logical
 * timer and same-thread executor, so we can produce deterministic test results.
 */
public class RelationalPlanCacheTests {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(RelationalPlanCacheTests.class, TestSchemas.books());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @Nonnull
    private static QueryPredicate gte1970p0(final int tokenIndex) {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1970));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1979));
        return PredicateWithValueAndRanges.ofRanges(new PromoteValue(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT, false)), Type.primitiveType(Type.TypeCode.INT), null), Set.of(builder.build().get()));
    }

    @Nonnull
    private static QueryPredicate gte1970p1(final int tokenIndex) {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1970));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1979));
        return PredicateWithValueAndRanges.ofRanges(new PromoteValue(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT, false)), Type.primitiveType(Type.TypeCode.INT), null), Set.of(builder.build().get()));
    }

    @Nonnull
    private static QueryPredicate gte1980p0(final int tokenIndex, @Nonnull Optional<String> scope) {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1980));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1989));
        return PredicateWithValueAndRanges.ofRanges(new PromoteValue(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex, scope), Type.primitiveType(Type.TypeCode.INT, false)), Type.primitiveType(Type.TypeCode.INT), null), Set.of(builder.build().get()));
    }

    @Nonnull
    private static QueryPredicate gte1980p1(final int tokenIndex) {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1980));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1989));
        return PredicateWithValueAndRanges.ofRanges(new PromoteValue(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT, false)), Type.primitiveType(Type.TypeCode.INT), null), Set.of(builder.build().get()));
    }

    @Nonnull
    private static QueryPredicate gte1990p0(final int tokenIndex) {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1990));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1999));
        return PredicateWithValueAndRanges.ofRanges(new PromoteValue(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT, false)), Type.primitiveType(Type.TypeCode.INT), null), Set.of(builder.build().get()));
    }

    @Nonnull
    private static QueryPredicate gte1990p1(final int tokenIndex) {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1990));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1999));
        return PredicateWithValueAndRanges.ofRanges(new PromoteValue(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT, false)), Type.primitiveType(Type.TypeCode.INT), null), Set.of(builder.build().get()));
    }

    // todo (yhatem) clean this up.
    @Nonnull
    private static QueryPredicate ofTypeIntp0(final int tokenIndex) {
        return new ValuePredicate(OfTypeValue.of(ConstantObjectValue.of(Quantifier.constant(),
                        constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT)), Type.primitiveType(Type.TypeCode.INT, false)),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));
    }

    @Nonnull
    private static QueryPredicate ofTypeIntp1(final int tokenIndex) {
        return ofTypeInt(tokenIndex, Optional.empty());
    }

    @Nonnull
    private static QueryPredicate strEq(final int tokenIndex1, final String scope1, final int tokenIndex2, final String scope2) {
        return strEq(tokenIndex1, Optional.of(scope1), tokenIndex2, Optional.of(scope2));
    }

    @Nonnull
    private static QueryPredicate strEq(final int tokenIndex1, final int tokenIndex2) {
        return strEq(tokenIndex1, Optional.empty(), tokenIndex2, Optional.empty());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private static QueryPredicate strEq(final int tokenIndex1, final Optional<String> scope1, final int tokenIndex2, final Optional<String> scope2) {
        final var leftCov = ConstantObjectValue.of(Quantifier.constant(), constantId(tokenIndex1, scope1), Type.primitiveType(Type.TypeCode.STRING));
        final var rightCov = ConstantObjectValue.of(Quantifier.constant(), constantId(tokenIndex2, scope2), Type.primitiveType(Type.TypeCode.STRING));

        final var leftIsNotNull = new ValuePredicate(leftCov, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
        final var rightIsNotNull = new ValuePredicate(rightCov, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
        final var equalityPredicate = new ValuePredicate(leftCov, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, rightCov));
        final var notNullComparison = AndPredicate.and(ImmutableList.of(leftIsNotNull, rightIsNotNull, equalityPredicate));

        final var leftIsNull = new ValuePredicate(leftCov, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
        final var rightIsNull = new ValuePredicate(rightCov, new Comparisons.NullComparison(Comparisons.Type.IS_NULL));
        final var bothAreNullComparison = AndPredicate.and(leftIsNull, rightIsNull);

        return OrPredicate.or(notNullComparison, bothAreNullComparison);
    }

    @Nonnull
    private static QueryPlanConstraint strEqCon(final int tokenIndex1, final String scope1, final int tokenIndex2, final String scope2) {
        return QueryPlanConstraint.ofPredicate(strEq(tokenIndex1, scope1, tokenIndex2, scope2));
    }

    @Nonnull
    private static QueryPlanConstraint strEqCon(final int tokenIndex1, final int tokenIndex2) {
        return QueryPlanConstraint.ofPredicate(strEq(tokenIndex1, tokenIndex2));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private static QueryPlanConstraint strEqCon(final int tokenIndex1, final Optional<String> scope1, final int tokenIndex2,
                                                final Optional<String> scope2) {
        return QueryPlanConstraint.ofPredicate(strEq(tokenIndex1, scope1, tokenIndex2, scope2));
    }

    @Nonnull
    private static QueryPlanConstraint isNotNullInt(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(isNotNull(tokenIndex, Optional.empty(), Type.primitiveType(Type.TypeCode.INT)));
    }

    @Nonnull
    private static QueryPlanConstraint isNotNullInt(final int tokenIndex, final String scope) {
        return QueryPlanConstraint.ofPredicate(isNotNull(tokenIndex, Optional.of(scope), Type.primitiveType(Type.TypeCode.INT)));
    }

    @Nonnull
    private static QueryPlanConstraint isNotNullStr(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(isNotNull(tokenIndex, Optional.empty(), Type.primitiveType(Type.TypeCode.STRING)));
    }

    @Nonnull
    private static QueryPlanConstraint isNotNullStr(final int tokenIndex, final String scope) {
        return QueryPlanConstraint.ofPredicate(isNotNull(tokenIndex, Optional.of(scope), Type.primitiveType(Type.TypeCode.STRING)));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private static QueryPredicate isNotNull(final int tokenIndex, final Optional<String> scope, Type type) {
        return new ValuePredicate(EvaluatesToValue.isNotNull(
                ConstantObjectValue.of(Quantifier.constant(), constantId(tokenIndex, scope), type)),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private static QueryPredicate ofTypeInt(final int tokenIndex, final Optional<String> scope) {
        return new ValuePredicate(OfTypeValue.of(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex, scope), Type.primitiveType(Type.TypeCode.INT)), Type.primitiveType(Type.TypeCode.INT, false)),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));
    }

    @Nonnull
    private static QueryPredicate ofTypeString(final int tokenIndex, final String scope) {
        return ofTypeString(tokenIndex, Optional.of(scope));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private static QueryPredicate ofTypeString(final int tokenIndex, final Optional<String> scope) {
        return new ValuePredicate(OfTypeValue.of(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex, scope), Type.primitiveType(Type.TypeCode.STRING)), Type.primitiveType(Type.TypeCode.STRING, false)),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));
    }

    @Nonnull
    private static QueryPlanConstraint ofTypeStringCons(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(ofTypeString(tokenIndex, Optional.empty()));
    }

    @Nonnull
    private static QueryPlanConstraint ofTypeStringCons(final int tokenIndex, final String scope) {
        return QueryPlanConstraint.ofPredicate(ofTypeString(tokenIndex, scope));
    }

    @Nonnull
    private static QueryPlanConstraint c1970Cp0(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(gte1970p0(tokenIndex));
    }

    @Nonnull
    private static QueryPlanConstraint c1970Cp1(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(gte1970p1(tokenIndex));
    }

    @Nonnull
    private static QueryPlanConstraint c1980Cp0(final int tokenIndex) {
        return c1980Cp0(tokenIndex, null);
    }

    @Nonnull
    private static QueryPlanConstraint c1980Cp0(final int tokenIndex, @Nullable String scope) {
        return QueryPlanConstraint.ofPredicate(gte1980p0(tokenIndex, Optional.ofNullable(scope)));
    }

    @Nonnull
    private static QueryPlanConstraint c1980Cp1(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(gte1980p1(tokenIndex));
    }

    @Nonnull
    private static QueryPlanConstraint c1990Cp0(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(gte1990p0(tokenIndex));
    }

    @Nonnull
    private static QueryPlanConstraint c1990Cp1(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(gte1990p1(tokenIndex));
    }

    @Nonnull
    private static QueryPlanConstraint ofTypeIntCp0(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(ofTypeIntp0(tokenIndex));
    }

    @Nonnull
    private static QueryPlanConstraint ofTypeIntCp1(final int tokenIndex) {
        return QueryPlanConstraint.ofPredicate(ofTypeIntp1(tokenIndex));
    }

    @Nonnull
    private static QueryPlanConstraint ofTypeIntCons(final int tokenIndex, @Nullable String scope) {
        return QueryPlanConstraint.ofPredicate(ofTypeInt(tokenIndex, Optional.ofNullable(scope)));
    }

    @Nonnull
    private static final QueryPlanConstraint tautology = QueryPlanConstraint.noConstraint();

    @Nonnull
    private static final String i1970 = "IDX_1970";

    @Nonnull
    private static final String i1980 = "IDX_1980";

    @Nonnull
    private static final String i1990 = "IDX_1990";

    @Nonnull
    private static final String i2000 = "IDX_2000";

    @Nonnull
    private static final String Scan = "SCAN";

    @Nonnull
    private PlannerConfiguration configOf(@Nonnull final Set<String> readableIndexes) {
        return configOf(readableIndexes, Options.none());
    }

    @Nonnull
    private PlannerConfiguration configOf(@Nonnull final Set<String> readableIndexes, @Nonnull final Options options) {
        return PlannerConfiguration.of(Optional.of(readableIndexes), options);
    }

    @Nonnull
    private PlanGenerator getPlanGenerator(@Nonnull final RelationalPlanCache cache,
                                           @Nonnull final String schemaTemplateName,
                                           int schemaTemplateVersion,
                                           int userVersion,
                                           @Nonnull final Set<String> readableIndexes,
                                           @Nonnull final Options options) throws Exception {
        final var schemaName = connection.getSchema();
        final var embeddedConnection = Assert.castUnchecked(connection.getUnderlyingEmbeddedConnection(), EmbeddedRelationalConnection.class);
        final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toBuilder().setVersion(schemaTemplateVersion).setName(schemaTemplateName).build();
        final AbstractDatabase database = embeddedConnection.getRecordLayerDatabase();
        final var storeState = new RecordStoreState(null, readableIndexes.stream().map(index -> Pair.of(index, IndexState.READABLE)).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        final FDBRecordStoreBase<?> store = database.loadSchema(schemaName).loadStore().unwrap(FDBRecordStoreBase.class);
        final PlanContext planContext = PlanContext.Builder
                .create()
                .fromDatabase(database)
                .fromRecordStore(store, options)
                .withSchemaTemplate(embeddedConnection.getTransaction().getBoundSchemaTemplateMaybe().orElse(schemaTemplate))
                .withMetricsCollector(Assert.notNullUnchecked(embeddedConnection.getMetricCollector()))
                .withPlannerConfiguration(PlannerConfiguration.of(Optional.of(readableIndexes), options))
                .withUserVersion(userVersion)
                .build();
        return PlanGenerator.create(Optional.of(cache), planContext, store.getRecordMetaData(), storeState, store.getIndexMaintainerRegistry(), options);
    }

    @Nonnull
    private Plan.ExecutionContext getExecutionContext() throws RelationalException {
        final var embeddedConnection = Assert.castUnchecked(connection.getUnderlyingEmbeddedConnection(), EmbeddedRelationalConnection.class);
        return Plan.ExecutionContext.of(embeddedConnection.getTransaction(),  Options.builder().build(),
                embeddedConnection, Assert.notNullUnchecked(embeddedConnection.getMetricCollector()));
    }

    @Nonnull
    private static String inferScanType(@Nonnull final Plan<?> plan) {
        Assertions.assertTrue(plan instanceof QueryPlan.PhysicalQueryPlan);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        final var desc = physicalPlan.getRecordQueryPlan().toString(); // very ad-hoc :/
        if (desc.contains(i1970)) {
            return i1970;
        } else if (desc.contains(i1980)) {
            return i1980;
        } else if (desc.contains(i1990)) {
            return i1990;
        } else if (desc.contains(i2000)) {
            return i2000;
        } else {
            return Scan;
        }
    }

    @Nonnull
    private static QueryPlanConstraint cons(@Nonnull final QueryPlanConstraint... constraints) {
        return QueryPlanConstraint.composeConstraints(Arrays.asList(constraints));
    }

    private static void shouldBe(@Nonnull final RelationalPlanCache cache, @Nonnull final Map<Tuple, Map<PhysicalPlanEquivalence, String>> expectedLayout) {
        Map<Tuple, Map<PhysicalPlanEquivalence, String>> result = new HashMap<>();
        for (String key : cache.getStats().getAllKeys()) {
            for (QueryCacheKey secondaryKey : cache.getStats().getAllSecondaryKeys(key)) {
                result.put(
                        new Tuple(
                                secondaryKey.getCanonicalQueryString(),
                                key,
                                secondaryKey.getSchemaTemplateVersion(),
                                secondaryKey.getUserVersion(),
                                secondaryKey.getPlannerConfiguration(),
                                secondaryKey.getAuxiliaryMetadata()),
                        cache.getStats().getAllTertiaryMappings(key, secondaryKey)
                                .entrySet()
                                .stream()
                                .map(k -> new AbstractMap.SimpleEntry<>(k.getKey(), inferScanType(k.getValue())))
                                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));
            }
        }

        org.assertj.core.api.Assertions.assertThat(result)
                .containsAllEntriesOf(expectedLayout)
                .hasSameSizeAs(expectedLayout);
    }

    @Nonnull
    private RelationalPlanCache getCache(@Nonnull final FakeTicker ticker) {
        final var relationalCache = RelationalPlanCache.newRelationalCacheBuilder()
                .setExecutor(Runnable::run)
                .setSize(2)
                .setSecondarySize(2)
                .setTertiarySize(2) // two slots only for tertiary cache items (à la plan-family in production).
                .setTtl(20)
                .setSecondaryTtl(10)
                .setTertiaryTtl(5)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTertiaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();
        return relationalCache;
    }

    @Nonnull
    private PhysicalPlanEquivalence ppe(@Nonnull final QueryPlanConstraint... constraints) {
        return PhysicalPlanEquivalence.of(QueryPlanConstraint.composeConstraints(Arrays.asList(constraints)));
    }

    private void planQuery(@Nonnull final RelationalPlanCache cache,
                           @Nonnull final String query,
                           @Nonnull final String schemaTemplateName,
                           int schemaTemplateVersion,
                           int userVersion,
                           @Nonnull final Set<String> readableIndexes,
                           @Nonnull final String expectedPhysicalPlan) throws Exception {
        planQuery(cache, query, schemaTemplateName, schemaTemplateVersion, userVersion, readableIndexes,
                Options.none(), expectedPhysicalPlan);
    }

    private void planQuery(@Nonnull final RelationalPlanCache cache,
                           @Nonnull final String query,
                           @Nonnull final String schemaTemplateName,
                           int schemaTemplateVersion,
                           int userVersion,
                           @Nonnull final Set<String> readableIndexes,
                           @Nonnull final Options options, @Nonnull final String expectedPhysicalPlan) throws Exception {
        planQueryWithTemporaryFunctionsPreamble(cache, ImmutableList.of(), query, schemaTemplateName, schemaTemplateVersion,
                userVersion, readableIndexes, options, expectedPhysicalPlan);
    }

    private void planQueryWithTemporaryFunctionsPreamble(@Nonnull final RelationalPlanCache cache,
                                                           @Nonnull final List<String> temporaryFunctionsDefinitions,
                                                           @Nonnull final String query,
                                                           @Nonnull final String schemaTemplateName,
                                                           int schemaTemplateVersion,
                                                           int userVersion,
                                                           @Nonnull final Set<String> readableIndexes,
                                                           @Nonnull final String expectedPhysicalPlan) throws Exception {
        planQueryWithTemporaryFunctionsPreamble(cache, temporaryFunctionsDefinitions, query, schemaTemplateName,
                schemaTemplateVersion, userVersion, readableIndexes, Options.none(), expectedPhysicalPlan);
    }

    private void planQueryWithTemporaryFunctionsPreamble(@Nonnull final RelationalPlanCache cache,
                                                         @Nonnull final List<String> temporaryFunctionsDefinitions,
                                                         @Nonnull final String query,
                                                         @Nonnull final String schemaTemplateName,
                                                         int schemaTemplateVersion,
                                                         int userVersion,
                                                         @Nonnull final Set<String> readableIndexes,
                                                         @Nonnull final Options options,
                                                         @Nonnull final String expectedPhysicalPlan) throws Exception {
        connection.setAutoCommit(false);
        connection.getUnderlyingEmbeddedConnection().createNewTransaction();
        var planGenerator = getPlanGenerator(cache, schemaTemplateName, schemaTemplateVersion, userVersion, readableIndexes, options);
        for (var temporaryFunctionsDefinition : temporaryFunctionsDefinitions) {
            final var plan = planGenerator.getPlan(temporaryFunctionsDefinition);
            plan.execute(getExecutionContext());
            planGenerator = getPlanGenerator(cache, schemaTemplateName, schemaTemplateVersion, userVersion, readableIndexes, options);
        }
        final var physicalPlan = planGenerator.getPlan(query);
        connection.rollback();
        connection.setAutoCommit(true);
        Assertions.assertEquals(expectedPhysicalPlan, inferScanType(physicalPlan));
    }

    @Test
    void testCachingDifferentQueries() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // adding an entry to the cache to warm it up.
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // let's add an identical query with different boundaries, so we can use a different index (i1980).
        // however, we end up with in the same primary cache bucket because the primary key is identical.
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(
                        ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(cons(c1980Cp0(7), c1980Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1980)));

        // let's try another query, now the query itself is different -> must be a different entry in the _primary_ cache.
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 OR YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), Scan);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(
                        ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(cons(c1980Cp0(7), c1980Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1980),
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? OR \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                // tautology is expected since a primary scan accepts everything.
                Map.of(ppe(tautology, cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), Scan)
        ));
    }

    @Test
    void testCachingDifferentSchemaTemplateNames() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the schema template is different ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_2", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_2", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));
    }

    @Test
    void testCachingDifferentSchemaTemplateVersions() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the schema template version is different ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 11, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 11, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));
    }

    @Test
    void testCachingDifferentUserVersion() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the user version is different ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 101, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 101, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));
    }

    @Test
    void testCachingDifferentReadableIndexes() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the readable indexes set is different ...
        // this simulates situations of e.g. two users who upgraded to the _same_ schema template, however the groomer hasn't (yet) made more indexes (i1990) available for one of them
        // in that case, we want to make sure that we re-plan the query giving the optimizer a chance to consider the newly created index for producing a better plan.
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));
    }

    @Test
    void testCachingDifferentConstraints() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but which predicates that fall outside the ranges of the chosen index of the first query
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1983", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(
                        ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(cons(c1980Cp0(7), c1980Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1980)));
    }

    @Test
    void testEvictionFromPrimaryCache() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // 10 MS TTL for primary cache, if we pass 11 -> item must be evicted from primary cache.
        ticker.advance(Duration.of(11, ChronoUnit.MILLIS));
        shouldBe(cache, Map.of());
    }

    @Test
    void testEvictionFromPrimaryCacheWithLru() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues a different query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 OR YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), Scan);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970),
                // ... which is added as a separate entry in the main cache
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? OR \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(tautology, cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), Scan)));

        // customer 3 issues yet a different query, cache size is two, evicts an item ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), Scan);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(tautology, cons(ofTypeIntCp0(7), isNotNullInt(7))), Scan),
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));
    }

    @Test
    void testEvictionFromSecondaryCacheRemovesPrimaryKeyWhenEmpty() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // 10 MS TTL for primary cache, 5 MS TTL for secondary cache, if we pass 7 -> item must be evicted from secondary cache
        // and the removal listener will make sure to clean up the primary cache key as it becomes empty.
        ticker.advance(Duration.of(7, ChronoUnit.MILLIS));

        // this is needed because expiration is done passively for better performance.
        // cleanup causes expiration handling to kick in immediately resulting in deterministic behavior which is necessary for testing.
        cache.cleanUp();

        shouldBe(cache, Map.of());
    }

    @Test
    void testEvictionFromTertiaryCache() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // pass some time, so we have some jitter between secondary cache items necessary for producing deterministic test results.
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // customer 2 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(
                        ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(cons(c1980Cp0(7), c1980Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1980)));

        // 10 MS TTL for primary cache, 5 MS TTL for secondary cache, we already passed 2, now if pass 3 more we'll
        // evict the first cached item in the secondary cache, but _not_ the more recent one.
        ticker.advance(Duration.of(3, ChronoUnit.MILLIS));
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1980Cp0(7), c1980Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1980)));
    }

    @Test
    void testEvictionFromSecondaryCacheWithLru() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), ""),
                Map.of(
                        ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(cons(c1980Cp0(7), c1980Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1980)));

        // secondary cache has capacity of two, attempting to add a third item causes an eviction (LRU).
        // customer 3 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1990 AND YEAR < 1992", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1990);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), ""),
                Map.of(
                        ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(cons(c1990Cp0(7), c1990Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1990)));
    }

    @Test
    void testEvictionFromTertiaryCacheRemovesSecondaryKeyWhenEmpty() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11),
                        isNotNullInt(7), isNotNullInt(11))), i1970)));

        // 10 MS TTL for secondary cache, 5 MS TTL for tertiary cache, if we pass 7 -> item must be evicted from tertiary cache
        // and the removal listener will make sure to clean up the secondary cache key as it becomes empty.
        // As to not remove the primary cache entry, we issue another query on that schema template
        ticker.advance(Duration.of(7, ChronoUnit.MILLIS));
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), Scan);

        // this is needed because expiration is done passively for better performance.
        // cleanup causes expiration handling to kick in immediately resulting in deterministic behavior which is necessary for testing.
        cache.cleanUp();

        // Only one secondary key, the first one was removed
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(tautology, cons(ofTypeIntCp0(7), isNotNullInt(7))), Scan)));
    }

    @Test
    void testPlanReductionViaCosting() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11),
                        isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues a query that forces a "bad" plan within the same secondary cache entry of the plan above
        // because of scan boundaries that fall outside the range of the filtered index
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 2005 AND YEAR < 2010", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), Scan);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11),
                        isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(tautology, cons(ofTypeIntCp0(7), ofTypeIntCp1(11),
                                isNotNullInt(7), isNotNullInt(11))), Scan)));

        // we should still get back the "good" plan (scanning i1970)
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11),
                                isNotNullInt(7), isNotNullInt(11))), i1970,
                        ppe(tautology, cons(ofTypeIntCp0(7), ofTypeIntCp1(11),
                                isNotNullInt(7), isNotNullInt(11))), Scan)));
    }

    @Test
    void testConstraintsWithTemporaryFunction() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        planQueryWithTemporaryFunctionsPreamble(cache, ImmutableList.of(
                "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS() ON COMMIT DROP FUNCTION AS SELECT * FROM BOOKS WHERE TITLE LIKE 'SCIFI'",
                "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS_OF_80S() ON COMMIT DROP FUNCTION AS SELECT * FROM SCI_FI_BOOKS() WHERE YEAR > 1980 AND YEAR < 1989"),
                "SELECT * FROM SCI_FI_BOOKS_OF_80S()", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1980);

        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"SCI_FI_BOOKS_OF_80S\" ( ) ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), "CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"BOOKS\" WHERE \"TITLE\" LIKE 'SCIFI' ||CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS_OF_80S\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"SCI_FI_BOOKS\" ( ) WHERE \"YEAR\" > ? AND \"YEAR\" < ? "),
                Map.of(ppe(cons(
                        cons(c1980Cp0(20, "SCI_FI_BOOKS_OF_80S"), c1980Cp0(24, "SCI_FI_BOOKS_OF_80S")),
                        cons(ofTypeStringCons(18, "SCI_FI_BOOKS"), ofTypeIntCons(20, "SCI_FI_BOOKS_OF_80S"),
                            ofTypeIntCons(24, "SCI_FI_BOOKS_OF_80S"),
                            isNotNullStr(18, "SCI_FI_BOOKS"),
                            isNotNullInt(20, "SCI_FI_BOOKS_OF_80S"),
                            isNotNullInt(24, "SCI_FI_BOOKS_OF_80S")
                        )
                )), i1980)));
        // the fact that we generate a type constraint for like pattern, and we still keep it in the normalized query is related
        // to an orthogonal bug: https://github.com/FoundationDB/fdb-record-layer/issues/3389
    }

    @Test
    void testConstraintsWithTemporaryFunctionsIncludingUnusedOnes() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        planQueryWithTemporaryFunctionsPreamble(cache, ImmutableList.of(
                        "CREATE TEMPORARY FUNCTION OTHER_BOOKS() ON COMMIT DROP FUNCTION AS SELECT * FROM BOOKS WHERE TITLE LIKE 'OTHER'",
                        "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS() ON COMMIT DROP FUNCTION AS SELECT * FROM BOOKS WHERE TITLE LIKE 'SCIFI'",
                        "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS_OF_80S() ON COMMIT DROP FUNCTION AS SELECT * FROM SCI_FI_BOOKS() WHERE YEAR > 1980 AND YEAR < 1989"),
                "SELECT * FROM SCI_FI_BOOKS_OF_80S()", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1980);

        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"SCI_FI_BOOKS_OF_80S\" ( ) ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), "CREATE TEMPORARY FUNCTION \"OTHER_BOOKS\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"BOOKS\" WHERE \"TITLE\" LIKE 'OTHER' ||CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"BOOKS\" WHERE \"TITLE\" LIKE 'SCIFI' ||CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS_OF_80S\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"SCI_FI_BOOKS\" ( ) WHERE \"YEAR\" > ? AND \"YEAR\" < ? "),
                Map.of(ppe(cons(
                        cons(c1980Cp0(20, "SCI_FI_BOOKS_OF_80S"), c1980Cp0(24, "SCI_FI_BOOKS_OF_80S")),
                        cons(ofTypeStringCons(18, "SCI_FI_BOOKS"), ofTypeIntCons(20, "SCI_FI_BOOKS_OF_80S"),
                                ofTypeIntCons(24, "SCI_FI_BOOKS_OF_80S"),
                                isNotNullStr(18, "SCI_FI_BOOKS"),
                                isNotNullInt(20, "SCI_FI_BOOKS_OF_80S"),
                                isNotNullInt(24, "SCI_FI_BOOKS_OF_80S")
                        )
                )), i1980)));
    }

    @Test
    void testConstraintsWithTemporaryFunctionsMultipleReferences() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        planQueryWithTemporaryFunctionsPreamble(cache, ImmutableList.of(
                        "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS() ON COMMIT DROP FUNCTION AS SELECT * FROM BOOKS WHERE TITLE LIKE 'SCIFI'",
                        "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS_OF_80S() ON COMMIT DROP FUNCTION AS SELECT * FROM SCI_FI_BOOKS() WHERE YEAR > 1980 AND YEAR < 1989"),
                "SELECT * FROM SCI_FI_BOOKS_OF_80S(), SCI_FI_BOOKS_OF_80S()", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1980);

        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"SCI_FI_BOOKS_OF_80S\" ( ) , \"SCI_FI_BOOKS_OF_80S\" ( ) ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)),
                        "CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"BOOKS\" WHERE \"TITLE\" LIKE 'SCIFI' ||CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS_OF_80S\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"SCI_FI_BOOKS\" ( ) WHERE \"YEAR\" > ? AND \"YEAR\" < ? "),
                Map.of(ppe(cons(
                        cons(cons(c1980Cp0(20, "SCI_FI_BOOKS_OF_80S"), c1980Cp0(24, "SCI_FI_BOOKS_OF_80S")), cons(c1980Cp0(20, "SCI_FI_BOOKS_OF_80S"), c1980Cp0(24, "SCI_FI_BOOKS_OF_80S"))),
                        cons(ofTypeStringCons(18, "SCI_FI_BOOKS"), ofTypeIntCons(20, "SCI_FI_BOOKS_OF_80S"),
                                ofTypeIntCons(24, "SCI_FI_BOOKS_OF_80S"),
                                isNotNullStr(18, "SCI_FI_BOOKS"),
                                isNotNullInt(20, "SCI_FI_BOOKS_OF_80S"),
                                isNotNullInt(24, "SCI_FI_BOOKS_OF_80S")
                        ))
                ), i1980)));
    }

    @Test
    void testConstraintsWithTemporaryFunctionsMultipleLiterals() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        planQueryWithTemporaryFunctionsPreamble(cache, ImmutableList.of(
                        "CREATE TEMPORARY FUNCTION OTHER_BOOKS() ON COMMIT DROP FUNCTION AS SELECT * FROM BOOKS WHERE TITLE LIKE 'OTHER'",
                        "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS() ON COMMIT DROP FUNCTION AS SELECT * FROM BOOKS WHERE TITLE LIKE 'SCIFI'",
                        "CREATE TEMPORARY FUNCTION SCI_FI_BOOKS_OF_80S() ON COMMIT DROP FUNCTION AS SELECT * FROM SCI_FI_BOOKS() WHERE YEAR > 1980 AND YEAR < 1989"),
                "SELECT * FROM SCI_FI_BOOKS_OF_80S() AS A, OTHER_BOOKS() AS B WHERE A.YEAR > 1985 AND A.TITLE = 'OTHER'", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1980);

        shouldBe(cache, Map.of(new Tuple("SELECT * FROM \"SCI_FI_BOOKS_OF_80S\" ( ) AS \"A\" , \"OTHER_BOOKS\" ( ) AS \"B\" WHERE \"A\" . \"YEAR\" > ? AND \"A\" . \"TITLE\" = ? ",
                        "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980, i1990)), "CREATE TEMPORARY FUNCTION \"OTHER_BOOKS\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"BOOKS\" WHERE \"TITLE\" LIKE 'OTHER' ||CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"BOOKS\" WHERE \"TITLE\" LIKE 'SCIFI' ||CREATE TEMPORARY FUNCTION \"SCI_FI_BOOKS_OF_80S\" ( ) " +
                        "ON COMMIT DROP FUNCTION AS SELECT * FROM \"SCI_FI_BOOKS\" ( ) WHERE \"YEAR\" > ? AND \"YEAR\" < ? "),
                Map.of(ppe(cons(
                        cons(c1980Cp0(20, "SCI_FI_BOOKS_OF_80S"), c1980Cp0(24, "SCI_FI_BOOKS_OF_80S"), c1980Cp0(19)),
                        cons(ofTypeStringCons(18, "SCI_FI_BOOKS"), ofTypeIntCons(20, "SCI_FI_BOOKS_OF_80S"),
                                ofTypeIntCons(24, "SCI_FI_BOOKS_OF_80S"), ofTypeStringCons(18, "OTHER_BOOKS"),
                                ofTypeIntCp0(19), ofTypeStringCons(25),
                                strEqCon(25, Optional.empty(), 18, Optional.of("OTHER_BOOKS")),
                                isNotNullStr(18, "SCI_FI_BOOKS"),
                                isNotNullInt(20, "SCI_FI_BOOKS_OF_80S"),
                                isNotNullInt(24, "SCI_FI_BOOKS_OF_80S"),
                                isNotNullStr(18, "OTHER_BOOKS"),
                                isNotNullInt(19),
                                isNotNullStr(25)
                        ))
                ), i1980)));
    }

    @Test
    void testPlanningQueryWithAndWithoutDisabledPlannerRewriteRules() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);
        final var disabledRulesOption = Options.builder().withOption(Options.Name.DISABLE_PLANNER_REWRITING, true).build();

        // customer 1 issues a query with disabled planner rules
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), disabledRulesOption, i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980), disabledRulesOption), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // customer 2 issues the same query, with enabled planner rules.
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);

        // cache should contain two entries
        shouldBe(cache, Map.of(
                // ... one with disabled planner rules.
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980), disabledRulesOption), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970),
                // ... another one with enabled planner rules.
                new Tuple("SELECT * FROM \"BOOKS\" WHERE \"YEAR\" > ? AND \"YEAR\" < ? ", "SCHEMA_TEMPLATE_1", 10, 100, configOf(Set.of(i1970, i1980)), ""),
                Map.of(ppe(cons(c1970Cp0(7), c1970Cp1(11)), cons(ofTypeIntCp0(7), ofTypeIntCp1(11), isNotNullInt(7), isNotNullInt(11))), i1970)));

        // this is needed because expiration is done passively for better performance.
        // cleanup causes expiration handling to kick in immediately resulting in deterministic behavior which is necessary for testing.
        cache.cleanUp();
    }
}
