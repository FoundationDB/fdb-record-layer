/*
 * RelationalPlanCacheTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.OfTypeValue;
import com.apple.foundationdb.relational.api.Options;
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
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.google.common.testing.FakeTicker;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, RelationalPlanCacheTests.class, TestSchemas.books());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @Nonnull
    private static final QueryPredicate gte1970p0;

    static {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1970));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1979));
        gte1970p0 = PredicateWithValueAndRanges.ofRanges(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)), Set.of(builder.build().get()));
    }

    @Nonnull
    private static final QueryPredicate gte1970p1;

    static {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1970));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1979));
        gte1970p1 = PredicateWithValueAndRanges.ofRanges(ConstantObjectValue.of(Quantifier.constant(), 1, Type.primitiveType(Type.TypeCode.INT)), Set.of(builder.build().get()));
    }

    @Nonnull
    private static final QueryPredicate gt1980p0;

    static {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1980));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1989));
        gt1980p0 = PredicateWithValueAndRanges.ofRanges(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)), Set.of(builder.build().get()));
    }

    @Nonnull
    private static final QueryPredicate gte1980p1;

    static {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1980));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1989));
        gte1980p1 = PredicateWithValueAndRanges.ofRanges(ConstantObjectValue.of(Quantifier.constant(), 1, Type.primitiveType(Type.TypeCode.INT)), Set.of(builder.build().get()));
    }

    @Nonnull
    private static final QueryPredicate gte1990p0;

    static {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1990));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1999));
        gte1990p0 = PredicateWithValueAndRanges.ofRanges(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)), Set.of(builder.build().get()));
    }

    @Nonnull
    private static final QueryPredicate gte1990p1;

    static {
        final var builder = RangeConstraints.newBuilder();
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1990));
        builder.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1999));
        gte1990p1 = PredicateWithValueAndRanges.ofRanges(ConstantObjectValue.of(Quantifier.constant(), 1, Type.primitiveType(Type.TypeCode.INT)), Set.of(builder.build().get()));
    }

    @Nonnull
    private static final QueryPredicate ofTypeIntp0 = new ValuePredicate(OfTypeValue.of(ConstantObjectValue.of(Quantifier.constant(), 0, Type.primitiveType(Type.TypeCode.INT)), Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));

    @Nonnull
    private static final QueryPredicate ofTypeIntp1 = new ValuePredicate(OfTypeValue.of(ConstantObjectValue.of(Quantifier.constant(), 1, Type.primitiveType(Type.TypeCode.INT)), Type.primitiveType(Type.TypeCode.INT)),
            new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true));

    @Nonnull
    private static final QueryPlanConstraint c1970Cp0 = QueryPlanConstraint.ofPredicate(gte1970p0);

    @Nonnull
    private static final QueryPlanConstraint c1970Cp1 = QueryPlanConstraint.ofPredicate(gte1970p1);

    @Nonnull
    private static final QueryPlanConstraint c1980Cp0 = QueryPlanConstraint.ofPredicate(gt1980p0);

    @Nonnull
    private static final QueryPlanConstraint c1980Cp1 = QueryPlanConstraint.ofPredicate(gte1980p1);

    @Nonnull
    private static final QueryPlanConstraint c1990Cp0 = QueryPlanConstraint.ofPredicate(gte1990p0);

    @Nonnull
    private static final QueryPlanConstraint c1990Cp1 = QueryPlanConstraint.ofPredicate(gte1990p1);

    @Nonnull
    private static final QueryPlanConstraint ofTypeIntCp0 = QueryPlanConstraint.ofPredicate(ofTypeIntp0);

    @Nonnull
    private static final QueryPlanConstraint ofTypeIntCp1 = QueryPlanConstraint.ofPredicate(ofTypeIntp1);

    @Nonnull
    private static final QueryPlanConstraint tautology = QueryPlanConstraint.tautology();

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

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private Triple<PlanContext, PlanGenerator, BitSet> getPlanGenerator(@Nonnull final RelationalPlanCache cache,
                                                                @Nonnull final String schemaTemplateName,
                                                                int schemaTemplateVersion,
                                                                int userVersion,
                                                                @Nonnull final Set<String> readableIndexes) throws Exception {
        final var schemaName = connection.getSchema();
        final var embeddedConnection = connection.getUnderlying().unwrap(EmbeddedRelationalConnection.class);
        final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toBuilder().setVersion(schemaTemplateVersion).setName(schemaTemplateName).build();
        final AbstractDatabase database = embeddedConnection.getRecordLayerDatabase();
        final var storeState = new RecordStoreState(null, readableIndexes.stream().map(index -> Pair.of(index, IndexState.READABLE)).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        final FDBRecordStoreBase<Message> store = database.loadSchema(schemaName).loadStore().unwrap(FDBRecordStoreBase.class);
        final PlanContext planContext = PlanContext.Builder
                .create()
                .fromDatabase(database)
                .fromRecordStore(store)
                .withSchemaTemplate(schemaTemplate)
                .withMetricsCollector(embeddedConnection.getMetricCollector())
                .withPlannerConfiguration(PlannerConfiguration.from(Optional.of(readableIndexes)))
                .withUserVersion(userVersion)
                .build();
        return Triple.of(planContext, PlanGenerator.of(Optional.of(cache),
                        store.getRecordMetaData(), storeState, Options.builder().build()),
                schemaTemplate.getIndexEntriesAsBitset(Optional.of(readableIndexes)));
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
        return QueryPlanConstraint.compose(Arrays.asList(constraints));
    }

    private static void shouldBe(@Nonnull final RelationalPlanCache cache, @Nonnull final Map<Tuple, Map<PhysicalPlanEquivalence, String>> expectedLayout) {
        final Set<QueryCacheKey> keys = cache.getStats().getAllKeys();
        final Map<Tuple, Map<PhysicalPlanEquivalence, String>> result = keys
                .stream()
                .map(key -> new AbstractMap.SimpleEntry<>(key, cache.getStats().getAllSecondaryMappings(key)))
                .collect(Collectors.toMap(entry ->
                                new Tuple(
                                        entry.getKey().getCanonicalQueryString(),
                                        entry.getKey().getSchemaTemplateName(),
                                        entry.getKey().getSchemaTemplateVersion(),
                                        entry.getKey().getUserVersion(),
                                        entry.getKey().getReadableIndexes()),
                        entry -> entry
                                .getValue()
                                .entrySet()
                                .stream()
                                .map(k -> new AbstractMap.SimpleEntry<>(k.getKey(), inferScanType(k.getValue())))
                                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue))));
        org.assertj.core.api.Assertions.assertThat(result).isEqualTo(expectedLayout);
    }

    @Nonnull
    private RelationalPlanCache getCache(@Nonnull final FakeTicker ticker) {
        final var relationalCache = RelationalPlanCache.newRelationalCacheBuilder()
                .setExecutor(Runnable::run)
                .setSize(2)
                .setSecondarySize(2) // two slots only for secondary cache items (à la plan-family in production).
                .setTtl(10)
                .setSecondaryTtl(5)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();
        return relationalCache;
    }

    @Nonnull
    private PhysicalPlanEquivalence ppe(@Nonnull final QueryPlanConstraint... constraints) {
        return PhysicalPlanEquivalence.of(QueryPlanConstraint.compose(Arrays.asList(constraints)));
    }

    private BitSet planQuery(@Nonnull final RelationalPlanCache cache,
                             @Nonnull final String query,
                             @Nonnull final String schemaTemplateName,
                             int schemaTemplateVerison,
                             int userVersion,
                             @Nonnull final Set<String> readableIndexes,
                             @Nonnull final String expectedPhysicalPlan) throws Exception {
        final var input = getPlanGenerator(cache, schemaTemplateName, schemaTemplateVerison, userVersion, readableIndexes);
        final var planContext = input.getLeft();
        final var planGenerator = input.getMiddle();
        final var readableIndexesBitset = input.getRight();
        final var physicalPlan = planGenerator.getPlan(query, planContext);
        Assertions.assertEquals(expectedPhysicalPlan, inferScanType(physicalPlan));
        return readableIndexesBitset;
    }

    @Test
    void testCachingDifferentQueries() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // adding an entry to the cache to warm it up.
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // let's add an identical query with different boundaries, so we can use a different index (i1980).
        // however, we end up with in the same primary cache bucket because the primary key is identical.
        final var readableIndexesBitset2 = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset2),
                Map.of(
                        ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970,
                        ppe(cons(c1980Cp0, c1980Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1980)));

        // let's try another query, now the query itself is different -> must be a different entry in the _primary_ cache.
        final var readableIndexesBitset3 = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 OR YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), Scan);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset3),
                Map.of(
                        ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970,
                        ppe(cons(c1980Cp0, c1980Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1980),
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? OR YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset3),
                // tautology is expected since a primary scan accepts everything.
                Map.of(ppe(tautology, cons(ofTypeIntCp0, ofTypeIntCp1)), Scan)
        ));
    }

    @Test
    void testCachingDifferentSchemaTemplateNames() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the schema template is different ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_2", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_2", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));
    }

    @Test
    void testCachingDifferentSchemaTemplateVersions() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the schema template version is different ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 11, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 11, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));
    }

    @Test
    void testCachingDifferentUserVersion() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the user version is different ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 101, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 101, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));
    }

    @Test
    void testCachingDifferentReadableIndexes() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but the readable indexes set is different ...
        // this simulates situations of e.g. two users who upgraded to the _same_ schema template, however the groomer hasn't (yet) made more indexes (i1990) available for one of them
        // in that case, we want to make sure that we re-plan the query giving the optimizer a chance to consider the newly created index for producing a better plan.
        final var readableIndexesBitset2 = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970),
                // so it is added as a separate entry in the main cache
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset2),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));
    }

    @Test
    void testCachingDifferentConstraints() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues exactly the same query, the environment is identical to first query, but which predicates that fall outside the ranges of the chosen index of the first query
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1983", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(
                        ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970,
                        ppe(cons(c1980Cp0, c1980Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1980)));
    }

    @Test
    void testEvictionFromPrimaryCache() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // 10 MS TTL for primary cache, if we pass 11 -> item must be evicted from primary cache.
        ticker.advance(Duration.of(11, ChronoUnit.MILLIS));
        shouldBe(cache, Map.of());
    }

    @Test
    void testEvictionFromPrimaryCacheWithLru() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues a different query ...
        final var readableIndexesBitset2 = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 OR YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), Scan);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970),
                // ... which is added as a separate entry in the main cache
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? OR YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset2),
                Map.of(ppe(tautology, cons(ofTypeIntCp0, ofTypeIntCp1)), Scan)));

        // customer 3 issues yet a different query, cache size is two, evicts an item ...
        final var readableIndexesBitset3 = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), Scan);
        shouldBe(cache, Map.of(
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset3),
                Map.of(ppe(tautology, cons(ofTypeIntCp0)), Scan),
                new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));
    }

    @Test
    void testEvictionFromSecondaryCacheRemovesPrimaryKeyWhenEmpty() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // 10 MS TTL for primary cache, 5 MS TTL for secondary cache, if we pass 7 -> item must be evicted from secondary cache
        // and the removal listener will make sure to clean up the primary cache key as it becomes empty.
        ticker.advance(Duration.of(7, ChronoUnit.MILLIS));

        // this is needed because expiration is done passively for better performance.
        // cleanup causes expiration handling to kick in immediately resulting in deterministic behavior which is necessary for testing.
        cache.cleanUp();

        shouldBe(cache, Map.of());
    }

    @Test
    void testEvictionFromSecondaryCache() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // pass some time, so we have some jitter between secondary cache items necessary for producing deterministic test results.
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // customer 2 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(
                        ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970,
                        ppe(cons(c1980Cp0, c1980Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1980)));

        // 10 MS TTL for primary cache, 5 MS TTL for secondary cache, we already passed 2, now if pass 3 more we'll
        // evict the first cached item in the secondary cache, but _not_ the more recent one.
        ticker.advance(Duration.of(3, ChronoUnit.MILLIS));
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1980Cp0, c1980Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1980)));
    }

    @Test
    void testEvictionFromSecondaryCacheWithLru() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1985", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1980);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(
                        ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970,
                        ppe(cons(c1980Cp0, c1980Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1980)));

        // secondary cache has capacity of two, attempting to add a third item causes an eviction (LRU).
        // customer 3 issues a query ...
        planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1990 AND YEAR < 1992", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1990);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(
                        ppe(cons(c1980Cp0, c1980Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1980,
                        ppe(cons(c1990Cp0, c1990Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1990)));
    }

    @Test
    void testPlanReductionViaCosting() throws Exception {
        final var ticker = new FakeTicker();
        final var cache = getCache(ticker);

        // customer 1 issues a query ...
        final var readableIndexesBitset = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970)));

        // customer 2 issues a query that forces a "bad" plan within the same secondary cache entry of the plan above
        // because of scan boundaries that fall outside the range of the filtered index
        final var readableIndexesBitset2 = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 2005 AND YEAR < 2010", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), Scan);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset2),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970,
                        ppe(tautology, cons(ofTypeIntCp0, ofTypeIntCp1)), Scan)));

        // we should still get back the "good" plan (scanning i1970)
        final var readableIndexesBitset3 = planQuery(cache, "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979", "SCHEMA_TEMPLATE_1", 10, 100, Set.of(i1970, i1980, i1990), i1970);
        shouldBe(cache, Map.of(new Tuple("SELECT * FROM BOOKS WHERE YEAR > ? AND YEAR < ? ", "SCHEMA_TEMPLATE_1", 10, 100, readableIndexesBitset3),
                Map.of(ppe(cons(c1970Cp0, c1970Cp1), cons(ofTypeIntCp0, ofTypeIntCp1)), i1970,
                        ppe(tautology, cons(ofTypeIntCp0, ofTypeIntCp1)), Scan)));
    }
}
