/*
 * FDBCoveringIndexValuePlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.TupleSource;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IndexEntryToRecordValueHelper;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ScanWithFetchMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexValuePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RecordQueryCoveringIndexValuePlan}, the covering scan that reads an index entry by evaluating a value
 * rather than by running the copiers of an {@code IndexKeyValueToPartialRecord}. See
 * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2907">issue 2907</a>.
 * <p>
 * Nothing creates the plan yet -- it ships ahead of the planner that will emit it, so that the fleet learns to
 * deserialize it first. Every test therefore builds one by hand, over the very {@link RecordQueryIndexPlan} the planner
 * puts underneath a {@link RecordQueryCoveringIndexPlan} for the same query, which is what makes comparing the two
 * readers meaningful.
 * </p>
 */
@Tag(Tags.RequiresFDB)
class FDBCoveringIndexValuePlanTest extends FDBRecordStoreQueryTestBase {

    private static final String INDEX_NAME = "MySimpleRecord$num_value_unique";
    private static final String RECORD_TYPE_NAME = "MySimpleRecord";

    /**
     * An entry of {@code MySimpleRecord$num_value_unique} is the indexed field followed by the primary key, so the two
     * fields a covering scan can restore are read from those two key positions.
     */
    private static final Map<String, Integer> COVERED_KEY_ORDINALS = Map.of("num_value_unique", 0, "rec_no", 1);

    /**
     * The plan reads the same records as the covering plan the planner produced, which is the property the whole change
     * rests on: the value decodes an entry into what the copiers would have built.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void executePlanReadsTheSameRecordsAsTheCopiers() throws Exception {
        complexQuerySetup(null);
        final var coveringPlan = planCoveringQuery();
        final var valuePlan = coveringValuePlanOf(coveringPlan);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final var byCopiers = queryAsMaps(coveringPlan, Bindings.EMPTY_BINDINGS);
            final var byValue = queryAsMaps(valuePlan, Bindings.EMPTY_BINDINGS);
            assertFalse(byCopiers.isEmpty(), "the fixture has records above the bound, so the scan is not empty");
            assertEquals(byCopiers, byValue);
        }
    }

    /**
     * Everything the plan does not decide for itself it asks the index plan underneath it.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void delegatesToTheIndexPlanUnderneathIt() throws Exception {
        complexQuerySetup(null);
        final var valuePlan = coveringValuePlanOf(planCoveringQuery());
        final var indexPlan = valuePlan.getIndexPlan();

        assertEquals(INDEX_NAME, valuePlan.getIndexName());
        assertEquals(indexPlan.getScanType(), valuePlan.getScanType());
        assertEquals(indexPlan.isReverse(), valuePlan.isReverse());
        assertTrue(valuePlan.hasIndexScan(INDEX_NAME));
        assertFalse(valuePlan.hasIndexScan("some_other_index"));
        assertEquals(Set.of(INDEX_NAME), valuePlan.getUsedIndexes());
        assertEquals(indexPlan.getComplexity(), valuePlan.getComplexity());
        assertEquals(indexPlan.isStrictlySorted(), valuePlan.isStrictlySorted());
        assertEquals(indexPlan.getMatchCandidateMaybe(), valuePlan.getMatchCandidateMaybe());
        assertEquals(indexPlan.getCorrelatedTo(), valuePlan.getCorrelatedTo());
        assertEquals(indexPlan.canBeMinimized(), valuePlan.canBeMinimized());

        // a covering scan reads no records and loads nothing by key, whatever the index plan says
        assertFalse(valuePlan.hasRecordScan());
        assertFalse(valuePlan.hasFullRecordScan());
        assertFalse(valuePlan.hasLoadBykeys());
        assertEquals(AvailableFields.ALL_FIELDS, valuePlan.getAvailableFields());
        assertEquals(List.of(), valuePlan.getQuantifiers());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final RecordMetaData metaData = recordStore.getRecordMetaData();
            assertEquals(indexPlan.maxCardinality(metaData), valuePlan.maxCardinality(metaData));
        }
    }

    /**
     * Rewriting the plan -- to a strictly sorted one, or to a minimized one -- keeps the reader, neither of them
     * changing how an entry decodes.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void rewritingThePlanKeepsTheReader() throws Exception {
        complexQuerySetup(null);
        final var valuePlan = coveringValuePlanOf(planCoveringQuery());
        assertFalse(valuePlan.isStrictlySorted());

        final var strictlySorted = valuePlan.strictlySorted(Memoizer.noMemoization(PlannerStage.PLANNED));
        assertTrue(strictlySorted.isStrictlySorted());
        assertEquals(valuePlan.getIndexEntryToRecordValue(), strictlySorted.getIndexEntryToRecordValue());

        assertTrue(valuePlan.canBeMinimized(), "the planner left a match candidate on the index plan");
        final var minimized = valuePlan.minimize(ImmutableList.of());
        assertTrue(minimized.getMatchCandidateMaybe().isEmpty(), "minimizing drops the match candidate");
        assertEquals(valuePlan.getIndexEntryToRecordValue(), minimized.getIndexEntryToRecordValue());
    }

    /**
     * A value over the fetched record is pushed down to one over the index entry by the match candidate the index plan
     * carries, exactly as it is for the covering plan the planner produced.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void pushesAValueThroughTheFetch() throws Exception {
        complexQuerySetup(null);
        final var coveringPlan = planCoveringQuery();
        final var valuePlan = coveringValuePlanOf(coveringPlan);
        assertThat(valuePlan.getMatchCandidateMaybe().orElseThrow(), instanceOf(ScanWithFetchMatchCandidate.class));

        final var source = CorrelationIdentifier.of("source");
        final var target = CorrelationIdentifier.of("target");
        final var toBePushed = FieldValue.ofFieldName(QuantifiedObjectValue.of(source, baseTypeOf(coveringPlan)),
                "num_value_unique");

        final var pushed = valuePlan.pushValueThroughFetch(toBePushed, source, target);
        assertTrue(pushed.isPresent(), "the index entry carries the field, so the value pushes through");
        assertEquals(coveringPlan.pushValueThroughFetch(toBePushed, source, target), pushed);
    }

    /**
     * Two plans over the same index and the same reader are the same plan; changing either the record type or the
     * reader makes them different.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void equalPlansHaveEqualHashCodes() throws Exception {
        complexQuerySetup(null);
        final var coveringPlan = planCoveringQuery();
        final var valuePlan = coveringValuePlanOf(coveringPlan);
        final var same = coveringValuePlanOf(coveringPlan);

        assertEquals(valuePlan, same);
        assertEquals(valuePlan.hashCode(), same.hashCode());
        assertEquals(valuePlan.computeHashCodeWithoutChildren(), same.computeHashCodeWithoutChildren());
        assertNotEquals(valuePlan, coveringPlan, "a plan reading through a value is not one reading through copiers");

        final var otherRecordType = new RecordQueryCoveringIndexValuePlan(indexPlanOf(coveringPlan), "MyOtherRecord",
                valuePlan.getIndexEntryToRecordValue());
        assertNotEquals(valuePlan, otherRecordType);

        final var otherReader = new RecordQueryCoveringIndexValuePlan(indexPlanOf(coveringPlan), RECORD_TYPE_NAME,
                readerCovering(baseTypeOf(coveringPlan), Map.of("num_value_unique", 0)));
        assertNotEquals(valuePlan, otherReader);
    }

    /**
     * The legacy hash is the index plan's own, so that a plan hash recorded before covering scans were told apart still
     * matches; the continuation hash mixes in what kind of plan this is.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void planHashFollowsTheIndexPlanOnlyForTheLegacyMode() throws Exception {
        complexQuerySetup(null);
        final var coveringPlan = planCoveringQuery();
        final var indexPlan = indexPlanOf(coveringPlan);
        final var valuePlan = coveringValuePlanOf(coveringPlan);

        assertEquals(indexPlan.planHash(PlanHashable.CURRENT_LEGACY), valuePlan.planHash(PlanHashable.CURRENT_LEGACY));
        assertNotEquals(indexPlan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                valuePlan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertEquals(coveringValuePlanOf(coveringPlan).planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                valuePlan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    /**
     * The plan has to survive a round trip through its serialized form, which is the whole reason it ships before
     * anything creates it.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void survivesSerialization() throws Exception {
        complexQuerySetup(null);
        final var valuePlan = coveringValuePlanOf(planCoveringQuery());

        final var deserialized = verifySerialization(valuePlan);
        assertThat(deserialized, instanceOf(RecordQueryCoveringIndexValuePlan.class));
        assertEquals(valuePlan.getIndexEntryToRecordValue(),
                ((RecordQueryCoveringIndexValuePlan)deserialized).getIndexEntryToRecordValue());
    }

    /**
     * A translation that renames nothing leaves the plan alone; one that renames something rebuilds it around the
     * translated index plan, keeping the reader.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void translateCorrelationsOnlyRebuildsForARealTranslation() throws Exception {
        complexQuerySetup(null);
        final var valuePlan = coveringValuePlanOf(planCoveringQuery());

        assertSame(valuePlan, valuePlan.translateCorrelations(TranslationMap.empty(), false, ImmutableList.of()));

        final var translated = valuePlan.translateCorrelations(
                TranslationMap.ofAliases(CorrelationIdentifier.of("source"), CorrelationIdentifier.of("target")),
                false, ImmutableList.of());
        assertEquals(valuePlan, translated, "nothing in this plan is correlated, so the translation is a no-op");
        assertEquals(valuePlan.getIndexEntryToRecordValue(), translated.getIndexEntryToRecordValue());
    }

    /**
     * Every plan property looks through to the index plan, and so answers what it answers for the covering plan the
     * planner produced.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void propertiesLookThroughToTheIndexPlan() throws Exception {
        complexQuerySetup(null);
        final var coveringPlan = planCoveringQuery();
        final var valuePlan = coveringValuePlanOf(coveringPlan);

        assertEquals(CardinalitiesProperty.cardinalities().evaluate(coveringPlan),
                CardinalitiesProperty.cardinalities().evaluate(valuePlan));
        assertEquals(DerivationsProperty.derivations().evaluate(coveringPlan).getResultValues(),
                DerivationsProperty.derivations().evaluate(valuePlan).getResultValues());
        assertEquals(DistinctRecordsProperty.distinctRecords().evaluate(coveringPlan),
                DistinctRecordsProperty.distinctRecords().evaluate(valuePlan));
        assertEquals(OrderingProperty.ordering().evaluate(coveringPlan),
                OrderingProperty.ordering().evaluate(valuePlan));
        assertEquals(PrimaryKeyProperty.primaryKey().evaluate(coveringPlan),
                PrimaryKeyProperty.primaryKey().evaluate(valuePlan));
        assertTrue(StoredRecordProperty.storedRecord().evaluate(valuePlan),
                "a covering scan hands back what stands for a stored record");
    }

    /**
     * The plan counts as a covering index scan, explains as one, and draws as one.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void reportsItselfAsACoveringIndexScan() throws Exception {
        complexQuerySetup(null);
        final var valuePlan = coveringValuePlanOf(planCoveringQuery());

        final var timer = new FDBStoreTimer();
        valuePlan.logPlanStructure(timer);
        assertEquals(1, timer.getCount(FDBStoreTimer.Counts.PLAN_COVERING_INDEX));

        final var explained = valuePlan.toString();
        assertTrue(explained.startsWith("COVERING("), () -> "unexpected explain: " + explained);
        assertTrue(explained.contains(INDEX_NAME), () -> "unexpected explain: " + explained);
        assertTrue(explained.contains("KEY:[0] AS num_value_unique"),
                () -> "the explain should say where each field is read from: " + explained);

        // the graphical explain is what goes through rewritePlannerGraph
        assertTrue(PlannerGraphVisitor.internalGraphicalExplain(valuePlan).contains(INDEX_NAME));
    }

    /**
     * Plans the query the covering scans are built from: it reads only the indexed field and the primary key, both of
     * which an entry of {@code MySimpleRecord$num_value_unique} carries, so the planner covers it.
     * <p>
     * Planned directly rather than through {@code planQuery}, which round trips the plan through its serialized form --
     * and a deserialized index plan has lost its match candidate, which several of these tests are about.
     * </p>
     */
    @Nonnull
    private RecordQueryCoveringIndexPlan planCoveringQuery() {
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(RECORD_TYPE_NAME)
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Collections.singletonList(field("num_value_unique")))
                .build();
        final RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, instanceOf(RecordQueryCoveringIndexPlan.class));
        return (RecordQueryCoveringIndexPlan)plan;
    }

    @Nonnull
    private static RecordQueryIndexPlan indexPlanOf(@Nonnull final RecordQueryCoveringIndexPlan coveringPlan) {
        return (RecordQueryIndexPlan)coveringPlan.getIndexPlan();
    }

    @Nonnull
    private static Type.Record baseTypeOf(@Nonnull final RecordQueryCoveringIndexPlan coveringPlan) {
        return (Type.Record)Objects.requireNonNull(indexPlanOf(coveringPlan).getResultType().getInnerType());
    }

    /**
     * A plan reading the entries of the given covering plan's index through a value rather than through its copiers.
     */
    @Nonnull
    private static RecordQueryCoveringIndexValuePlan coveringValuePlanOf(
            @Nonnull final RecordQueryCoveringIndexPlan coveringPlan) {
        return new RecordQueryCoveringIndexValuePlan(indexPlanOf(coveringPlan), RECORD_TYPE_NAME,
                readerCovering(baseTypeOf(coveringPlan), COVERED_KEY_ORDINALS));
    }

    /**
     * A value reading the named fields out of the given entry key positions, leaving every other field of the record
     * type absent.
     */
    @Nonnull
    private static Value readerCovering(@Nonnull final Type.Record baseType,
                                        @Nonnull final Map<String, Integer> keyOrdinals) {
        final var baseObjectValue = QuantifiedObjectValue.of(Quantifier.current(), baseType);
        final var covered = new IndexEntryToRecordValueHelper();
        for (final var field : baseType.getFields()) {
            final var ordinal = keyOrdinals.get(field.getFieldName());
            if (ordinal != null) {
                covered.withChild(field.getFieldName()).cover(IndexEntryToRecordValueHelper.entryColumn(baseObjectValue,
                        field.getFieldName(), TupleSource.KEY, ordinal));
            }
        }
        return covered.toRecordValue(baseType);
    }
}
