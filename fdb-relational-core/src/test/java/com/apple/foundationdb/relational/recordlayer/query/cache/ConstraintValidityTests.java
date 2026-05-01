/*
 * ConstraintValidityTests.java
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.EvaluatesToValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OfTypeValue;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.FakeTicker;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.apple.foundationdb.relational.recordlayer.query.OrderedLiteral.constantId;

public class ConstraintValidityTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(ConstraintValidityTests.class, TestSchemas.score());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @Nonnull
    private static final String MaxScoreByGame10 = "MAXSCOREBYGAME10";

    @Nonnull
    private static final String MaxScoreByGame20 = "MAXSCOREBYGAME20";

    @Nonnull
    private static final String BitAndScore2 = "BITANDSCORE2";

    @Nonnull
    private static final String BitAndScore4 = "BITANDSCORE4";

    @Nonnull
    private static final String GameIdx = "GAMEIDX";

    @Nonnull
    private static final String Scan = "SCAN";

    @BeforeAll
    public static void beforeAll() {
        Utils.enableCascadesDebugger();
    }

    @Nonnull
    private PlanGenerator getPlanGenerator(@Nonnull final RelationalPlanCache cache) throws Exception {
        final var schemaName = connection.getSchema();
        final var embeddedConnection = connection.getUnderlyingEmbeddedConnection().unwrap(EmbeddedRelationalConnection.class);
        final var readableIndexes = ImmutableSet.of(MaxScoreByGame10, MaxScoreByGame20, BitAndScore2, BitAndScore4);
        final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toBuilder().setVersion(42).setName("SCHEMA_TEMPLATE").build();
        final AbstractDatabase database = embeddedConnection.getRecordLayerDatabase();
        final var storeState = new RecordStoreState(null, readableIndexes.stream().map(index -> Pair.of(index, IndexState.READABLE)).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        final FDBRecordStoreBase<?> store = database.loadSchema(schemaName).loadStore().unwrap(FDBRecordStoreBase.class);
        final PlanContext planContext = PlanContext.Builder
                .create()
                .fromDatabase(database)
                .fromRecordStore(store, Options.none())
                .withSchemaTemplate(schemaTemplate)
                .withMetricsCollector(embeddedConnection.getMetricCollector())
                .withUserVersion(44)
                .build();
        return PlanGenerator.create(Optional.of(cache), planContext, store.getRecordMetaData(), storeState, store.getIndexMaintainerRegistry(), Options.builder().build());
    }

    private void planQuery(@Nonnull final RelationalPlanCache cache,
                           @Nonnull final String query,
                           @Nonnull final String expectedPhysicalPlan) throws Exception {
        connection.setAutoCommit(false);
        connection.getUnderlyingEmbeddedConnection().createNewTransaction();
        final var planGenerator = getPlanGenerator(cache);
        final var plan = planGenerator.getPlan(query);
        connection.rollback();
        connection.setAutoCommit(true);
        Assertions.assertEquals(inferScanType(plan), expectedPhysicalPlan);
    }

    @Nonnull
    private RelationalPlanCache getCache(@Nonnull final FakeTicker ticker) {
        return RelationalPlanCache.newRelationalCacheBuilder()
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
    }

    @Nonnull
    private static String inferScanType(@Nonnull final Plan<?> plan) {
        Assertions.assertInstanceOf(QueryPlan.PhysicalQueryPlan.class, plan);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        final var desc = physicalPlan.getRecordQueryPlan().toString(); // very ad-hoc :/
        if (desc.contains(GameIdx)) { // not the best plan interpreter, gameIdx should be used in join queries only
            return GameIdx;
        } else if (desc.contains(MaxScoreByGame10)) {
            return MaxScoreByGame10;
        } else if (desc.contains(MaxScoreByGame20)) {
            return MaxScoreByGame20;
        } else if (desc.contains(BitAndScore2)) {
            return BitAndScore2;
        } else if (desc.contains(BitAndScore4)) {
            return BitAndScore4;
        } else {
            return Scan;
        }
    }

    @Nonnull
    private static QueryPredicate ofTypeInt(final int tokenIndex) {
        return new ValuePredicate(OfTypeValue.of(ConstantObjectValue.of(Quantifier.constant(),
                        constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT)), Type.primitiveType(Type.TypeCode.INT, false)),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private static QueryPredicate isNotNull(final int tokenIndex, final Optional<String> scope, Type type) {
        return new ValuePredicate(EvaluatesToValue.isNotNull(
                ConstantObjectValue.of(Quantifier.constant(), constantId(tokenIndex, scope), type)),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));
    }

    @Nonnull
    private static QueryPredicate isNotNullInt(final int tokenIndex) {
        return isNotNull(tokenIndex, Optional.empty(), Type.primitiveType(Type.TypeCode.INT));
    }

    @Nonnull
    private static QueryPredicate and(@Nonnull final QueryPredicate... predicates) {
        return AndPredicate.and(Arrays.asList(predicates));
    }

    @Nonnull
    private static QueryPredicate equalsConstraint(int tokenIndex, int value) {
        return new ValuePredicate(ConstantObjectValue.of(Quantifier.constant(),
                constantId(tokenIndex), Type.primitiveType(Type.TypeCode.INT)), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value));
    }

    @Nonnull
    private static QueryPredicate covsEqualsConstraints(int leftTokenIndex, int rightTokenIndex) {
        final var leftCov = ConstantObjectValue.of(Quantifier.constant(), constantId(leftTokenIndex), Type.primitiveType(Type.TypeCode.INT));
        final var rightCov = ConstantObjectValue.of(Quantifier.constant(), constantId(rightTokenIndex), Type.primitiveType(Type.TypeCode.INT));

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
    private static QueryPlanConstraint cons(@Nonnull final QueryPlanConstraint... constraints) {
        return QueryPlanConstraint.composeConstraints(Arrays.asList(constraints));
    }

    @Nonnull
    private static QueryPlanConstraint cons(@Nonnull final QueryPredicate... predicates) {
        return QueryPlanConstraint.ofPredicates(Arrays.asList(predicates));
    }

    @Nonnull
    private PhysicalPlanEquivalence ppe(@Nonnull final QueryPlanConstraint... constraints) {
        return PhysicalPlanEquivalence.of(QueryPlanConstraint.composeConstraints(Arrays.asList(constraints)));
    }

    private static void cacheShouldBe(@Nonnull RelationalPlanCache cache,
                                      @Nonnull Map<String, Map<PhysicalPlanEquivalence, String>> expectedLayout) {
        Map<String, Map<PhysicalPlanEquivalence, String>> result = new HashMap<>();
        for (var key : cache.getStats().getAllKeys()) {
            for (QueryCacheKey secondaryKey : cache.getStats().getAllSecondaryKeys(key)) {
                result.put(
                        secondaryKey.getCanonicalQueryString(),
                        cache.getStats().getAllTertiaryMappings(key, secondaryKey)
                                .entrySet()
                                .stream()
                                .map(k -> new AbstractMap.SimpleEntry<>(k.getKey(), inferScanType(k.getValue())))
                                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));
            }
        }

        org.assertj.core.api.Assertions.assertThat(result).isEqualTo(expectedLayout);
    }

    @Test
    void cachingQueryWithComplexGroupByExpressionBehavesCorrectlyCase1() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);
        final var c15Equals10 = equalsConstraint(15, 10);
        final var c15Int = ofTypeInt(15);
        final var c8Int = ofTypeInt(8);
        final var c15IntNotNull = isNotNullInt(15);
        final var c8IntNotNull = isNotNullInt(8);
        final var c8Equalsc15 = covsEqualsConstraints(8, 15);
        planQuery(cache, "SELECT MAX(score), game + 10 FROM score GROUP BY game + 10", MaxScoreByGame10);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" + ? FROM \"SCORE\" GROUP BY \"GAME\" + ? ",
                Map.of(ppe(cons(and(and(c15Equals10, c15Equals10), and(c15Int, c8Int, c8Equalsc15, c15IntNotNull, c8IntNotNull)))), MaxScoreByGame10)));

        final var c15Equals20 = equalsConstraint(15, 20);
        planQuery(cache, "SELECT MAX(score), game + 20 FROM score GROUP BY game + 20", MaxScoreByGame20);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" + ? FROM \"SCORE\" GROUP BY \"GAME\" + ? ",
                Map.of(
                        ppe(cons(and(and(c15Equals10, c15Equals10), and(c15Int, c8Int, c8Equalsc15, c15IntNotNull, c8IntNotNull)))), MaxScoreByGame10,
                        ppe(cons(and(and(c15Equals20, c15Equals20), and(c15Int, c8Int, c8Equalsc15, c15IntNotNull, c8IntNotNull)))), MaxScoreByGame20)));
    }

    @Test
    void cachingQueryWithComplexGroupByExpressionBehavesCorrectlyCase2() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);
        final var c15Equals2 = equalsConstraint(15, 2);
        final var c15Int = ofTypeInt(15);
        final var c8Int = ofTypeInt(8);
        final var c15IntNotNull = isNotNullInt(15);
        final var c8IntNotNull = isNotNullInt(8);
        final var c8Equalsc15 = covsEqualsConstraints(8, 15);
        planQuery(cache, "SELECT MAX(score), game & 2 FROM score GROUP BY game & 2", BitAndScore2);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" & ? FROM \"SCORE\" GROUP BY \"GAME\" & ? ",
                Map.of(ppe(cons(and(and(c15Equals2, c15Equals2), and(c15Int, c8Int, c8Equalsc15, c15IntNotNull, c8IntNotNull)))), BitAndScore2)));

        final var c15Equals4 = equalsConstraint(15, 4);
        planQuery(cache, "SELECT MAX(score), game & 4 FROM score GROUP BY game & 4", BitAndScore4);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" & ? FROM \"SCORE\" GROUP BY \"GAME\" & ? ",
                Map.of(
                        ppe(cons(and(and(c15Equals2, c15Equals2), and(c15Int, c8Int, c8Equalsc15, c15IntNotNull, c8IntNotNull)))), BitAndScore2,
                        ppe(cons(and(and(c15Equals4, c15Equals4), and(c15Int, c8Int, c8Equalsc15, c15IntNotNull, c8IntNotNull)))), BitAndScore4)));
    }

    @Test
    void cachingQueryWithComplexGroupByExpressionSubsumingIndexExpressionBehavesCorrectlyCase1() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);
        final var c17Equals10 = equalsConstraint(17, 10);
        final var c17Int = ofTypeInt(17);
        final var c10Int = ofTypeInt(10);
        final var c8Int = ofTypeInt(8);
        final var c17IntNotNull = isNotNullInt(17);
        final var c8IntNotNull = isNotNullInt(8);
        final var c10IntNotNull = isNotNullInt(10);
        final var c8Equalsc17 = covsEqualsConstraints(8, 17);
        planQuery(cache, "SELECT MAX(score), game + 10 + 42 FROM score GROUP BY game + 10", MaxScoreByGame10);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" + ? + ? FROM \"SCORE\" GROUP BY \"GAME\" + ? ",
                Map.of(ppe(cons(and(and(c17Equals10, c17Equals10), and(c17Int, c8Int, c10Int, c8Equalsc17, c17IntNotNull, c8IntNotNull, c10IntNotNull)))), MaxScoreByGame10)));

        final var c17Equals20 = equalsConstraint(17, 20);
        planQuery(cache, "SELECT MAX(score), game + 20 + 42 FROM score GROUP BY game + 20", MaxScoreByGame20);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" + ? + ? FROM \"SCORE\" GROUP BY \"GAME\" + ? ",
                Map.of(
                        ppe(cons(and(and(c17Equals10, c17Equals10), and(c17Int, c8Int, c10Int, c8Equalsc17, c17IntNotNull, c8IntNotNull, c10IntNotNull)))), MaxScoreByGame10,
                        ppe(cons(and(and(c17Equals20, c17Equals20), and(c17Int, c8Int, c10Int, c8Equalsc17, c17IntNotNull, c8IntNotNull, c10IntNotNull)))), MaxScoreByGame20)));
    }

    @Test
    void cachingQueryWithComplexGroupByExpressionSubsumingIndexExpressionBehavesCorrectlyCase2() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);
        final var c17Equals2 = equalsConstraint(17, 2);
        final var c17Int = ofTypeInt(17);
        final var c10Int = ofTypeInt(10);
        final var c8Int = ofTypeInt(8);
        final var c17IntNotNull = isNotNullInt(17);
        final var c8IntNotNull = isNotNullInt(8);
        final var c10IntNotNull = isNotNullInt(10);
        final var c8Equalsc15 = covsEqualsConstraints(8, 17);
        planQuery(cache, "SELECT MAX(score), game & 2 + 10 FROM score GROUP BY game & 2", BitAndScore2);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" & ? + ? FROM \"SCORE\" GROUP BY \"GAME\" & ? ",
                Map.of(ppe(cons(and(and(c17Equals2, c17Equals2), and(c17Int, c8Int, c10Int, c8Equalsc15, c17IntNotNull, c8IntNotNull, c10IntNotNull)))), BitAndScore2)));

        final var c17Equals4 = equalsConstraint(17, 4);
        planQuery(cache, "SELECT MAX(score), game & 4 + 10 FROM score GROUP BY game & 4", BitAndScore4);
        cacheShouldBe(cache, Map.of("SELECT MAX ( \"SCORE\" ) , \"GAME\" & ? + ? FROM \"SCORE\" GROUP BY \"GAME\" & ? ",
                Map.of(
                        ppe(cons(and(and(c17Equals2, c17Equals2), and(c17Int, c8Int, c10Int, c8Equalsc15, c17IntNotNull, c8IntNotNull, c10IntNotNull)))), BitAndScore2,
                        ppe(cons(and(and(c17Equals4, c17Equals4), and(c17Int, c8Int, c10Int, c8Equalsc15, c17IntNotNull, c8IntNotNull, c10IntNotNull)))), BitAndScore4)));
    }

    @Test
    void cachingQueryWithIsNotNullFilterWorksAsExpected() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);
        planQuery(cache, "SELECT game.name from score, game use index (gameIdx) where game.score_id is not null and game.score_id = score.id", GameIdx);
        cacheShouldBe(cache, Map.of("SELECT \"GAME\" . \"NAME\" from \"SCORE\" , \"GAME\" use index ( \"GAMEIDX\" ) " +
                "where \"GAME\" . \"SCORE_ID\" is not null and \"GAME\" . \"SCORE_ID\" = \"SCORE\" . \"ID\" ", Map.of(ppe(QueryPlanConstraint.noConstraint()), GameIdx)));
    }
}
