/*
 * PlanningCostModelVectorEngineTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.VectorIndexEnginePreference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link PlanningCostModel} honors the configured {@link VectorIndexEnginePreference} when choosing between
 * two vector index plans that are otherwise indistinguishable, and that it stays out of the way when no engine is
 * preferred.
 */
class PlanningCostModelVectorEngineTest {
    private static final String HNSW_INDEX_NAME = "hnswIndex";
    private static final String GUARDIANN_INDEX_NAME = "guardiannIndex";

    /**
     * The base type the plans are over. It has to carry the indexed field: comparing plans evaluates the cardinalities
     * property, which resolves the index's key expression against it.
     *
     * @return the base type
     */
    @Nonnull
    private static Type.Record baseType() {
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE), Optional.of("embedding"))));
    }

    @Nonnull
    private static RecordQueryIndexPlan vectorIndexPlan(@Nonnull final String indexName, final boolean guardiann) {
        final Index index = vectorIndex(indexName, guardiann);
        final Type.Record baseType = baseType();
        final VectorIndexScanMatchCandidate matchCandidate =
                new VectorIndexScanMatchCandidate(index,
                        Collections.emptyList(),
                        Traversal.withRoot(Reference.empty()),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableSet.of(),
                        baseType,
                        CorrelationIdentifier.of("base"),
                        field("embedding"),
                        null);
        return new RecordQueryIndexPlan(index.getName(),
                null,
                new IndexScanComparisons(IndexScanType.BY_VALUE, ScanComparisons.EMPTY),
                IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                false,
                false,
                matchCandidate,
                baseType,
                QueryPlanConstraint.noConstraint());
    }

    @Nonnull
    private static Index vectorIndex(@Nonnull final String indexName, final boolean guardiann) {
        return guardiann
               ? new Index(indexName, field("embedding"), IndexTypes.VECTOR,
                       Collections.singletonMap(IndexOptions.VECTOR_ENGINE, "GUARDIANN"))
               : new Index(indexName, field("embedding"), IndexTypes.VECTOR);
    }

    /**
     * An index plan over a value index, so it carries a match candidate — every index plan reaching the cost model does,
     * see {@code UnmatchedFieldsCountProperty} — but contributes no vector access.
     *
     * @param indexName the name of the index scanned
     * @return an index plan that is not a vector index scan
     */
    @Nonnull
    private static RecordQueryIndexPlan nonVectorIndexPlan(@Nonnull final String indexName) {
        final Index index = new Index(indexName, field("embedding"), IndexTypes.VALUE);
        final Type.Record baseType = baseType();
        final ValueIndexScanMatchCandidate matchCandidate =
                new ValueIndexScanMatchCandidate(index,
                        Collections.emptyList(),
                        Traversal.withRoot(Reference.empty()),
                        ImmutableList.of(),
                        baseType,
                        CorrelationIdentifier.of("base"),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        field("embedding"),
                        null);
        return new RecordQueryIndexPlan(index.getName(),
                null,
                new IndexScanComparisons(IndexScanType.BY_VALUE, ScanComparisons.EMPTY),
                IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                false,
                false,
                matchCandidate,
                baseType,
                QueryPlanConstraint.noConstraint());
    }

    @Nonnull
    private static PlanningCostModel costModel(@Nonnull final VectorIndexEnginePreference preference) {
        return new PlanningCostModel(RecordQueryPlannerConfiguration.builder()
                .setVectorIndexEnginePreference(preference)
                .build());
    }

    /**
     * Without a preference the two plans are indistinguishable to every cost criterion, so the comparison falls through
     * to the planHash tie-break — which is stable, but keyed on the index name and hence arbitrary. This is the behavior
     * the preference exists to override, so pin it down here.
     */
    @Test
    void noPreferenceFallsThroughToPlanHash() {
        final RecordQueryIndexPlan hnswPlan = vectorIndexPlan(HNSW_INDEX_NAME, false);
        final RecordQueryIndexPlan guardiannPlan = vectorIndexPlan(GUARDIANN_INDEX_NAME, true);
        final PlanningCostModel costModel = costModel(VectorIndexEnginePreference.NO_PREFERENCE);

        final int expected = Integer.compare(hnswPlan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                guardiannPlan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertEquals(expected, costModel.compare(hnswPlan, guardiannPlan));
    }

    @Test
    void preferHnswPicksTheHnswPlan() {
        final RecordQueryIndexPlan hnswPlan = vectorIndexPlan(HNSW_INDEX_NAME, false);
        final RecordQueryIndexPlan guardiannPlan = vectorIndexPlan(GUARDIANN_INDEX_NAME, true);
        final PlanningCostModel costModel = costModel(VectorIndexEnginePreference.PREFER_HNSW);

        assertTrue(costModel.compare(hnswPlan, guardiannPlan) < 0, "HNSW plan should sort first");
        assertTrue(costModel.compare(guardiannPlan, hnswPlan) > 0, "comparison should be antisymmetric");
    }

    @Test
    void preferGuardiannPicksTheGuardiannPlan() {
        final RecordQueryIndexPlan hnswPlan = vectorIndexPlan(HNSW_INDEX_NAME, false);
        final RecordQueryIndexPlan guardiannPlan = vectorIndexPlan(GUARDIANN_INDEX_NAME, true);
        final PlanningCostModel costModel = costModel(VectorIndexEnginePreference.PREFER_GUARDIANN);

        assertTrue(costModel.compare(guardiannPlan, hnswPlan) < 0, "Guardiann plan should sort first");
        assertTrue(costModel.compare(hnswPlan, guardiannPlan) > 0, "comparison should be antisymmetric");
    }

    /**
     * A preference must not order two plans over indexes of the <em>same</em> engine, otherwise it would start
     * overriding decisions the cost model is entitled to make on its own.
     */
    @Test
    void preferenceDoesNotSeparateTwoPlansOfThePreferredEngine() {
        final RecordQueryIndexPlan onePlan = vectorIndexPlan("guardiannIndexOne", true);
        final RecordQueryIndexPlan otherPlan = vectorIndexPlan("guardiannIndexTwo", true);

        assertSameAsWithoutPreference(onePlan, otherPlan, VectorIndexEnginePreference.PREFER_GUARDIANN);
    }

    /**
     * A preference for an engine that no candidate uses must leave the comparison alone: both plans are equally
     * non-preferred, so the planHash tie-break decides, exactly as it would with no preference set.
     */
    @Test
    void preferenceForAbsentEngineChangesNothing() {
        final RecordQueryIndexPlan onePlan = vectorIndexPlan("hnswIndexOne", false);
        final RecordQueryIndexPlan otherPlan = vectorIndexPlan("hnswIndexTwo", false);

        assertSameAsWithoutPreference(onePlan, otherPlan, VectorIndexEnginePreference.PREFER_GUARDIANN);
    }

    /**
     * The preference is about which engine backs a vector access, not about whether to make one at all. A plan that
     * makes no vector access must therefore not be ordered against one that does — otherwise preferring GuardiANN would
     * demote a plan scanning the only (HNSW) vector index available in favor of a plan that does not use a vector index.
     */
    @Test
    void preferenceDoesNotSeparateAVectorPlanFromANonVectorPlan() {
        final RecordQueryIndexPlan vectorPlan = vectorIndexPlan(HNSW_INDEX_NAME, false);
        final RecordQueryIndexPlan nonVectorPlan = nonVectorIndexPlan("valueIndex");

        for (final VectorIndexEnginePreference preference : VectorIndexEnginePreference.values()) {
            assertSameAsWithoutPreference(vectorPlan, nonVectorPlan, preference);
        }
    }

    /**
     * A member that makes more than one vector index access is not comparable on this criterion: the preference decides
     * which engine serves an access, and a pair of members that disagree on how many accesses they make is not the
     * like-for-like choice it is meant to decide. Left to the cost criteria instead.
     */
    @Test
    void preferenceAbstainsWhenOneSideMakesSeveralVectorAccesses() {
        final var twoGuardiannAccesses =
                planOpsMapOf(vectorIndexPlan("guardiannIndexOne", true), vectorIndexPlan("guardiannIndexTwo", true));
        final var oneHnswAccess = planOpsMapOf(vectorIndexPlan(HNSW_INDEX_NAME, false));

        final PlanningCostModel costModel = costModel(VectorIndexEnginePreference.PREFER_GUARDIANN);
        assertTrue(costModel.compareVectorIndexEnginePreference(twoGuardiannAccesses, oneHnswAccess).isEmpty());
        assertTrue(costModel.compareVectorIndexEnginePreference(oneHnswAccess, twoGuardiannAccesses).isEmpty());
    }

    /**
     * The guard itself: a plan making no vector access is not comparable to one that does, whichever preference is set.
     * Asserted on the criterion directly rather than through {@code compare()}, so that it cannot pass by coincidence
     * when the planHash tie-break happens to order the plans the same way the criterion would have.
     */
    @Test
    void preferenceAbstainsWhenOneSideMakesNoVectorAccess() {
        final var hnswPlan = planOpsMapOf(vectorIndexPlan(HNSW_INDEX_NAME, false));
        final var guardiannPlan = planOpsMapOf(vectorIndexPlan(GUARDIANN_INDEX_NAME, true));
        final var nonVectorPlan = planOpsMapOf(nonVectorIndexPlan("valueIndex"));

        for (final VectorIndexEnginePreference preference : VectorIndexEnginePreference.values()) {
            final PlanningCostModel costModel = costModel(preference);
            assertTrue(costModel.compareVectorIndexEnginePreference(hnswPlan, nonVectorPlan).isEmpty(),
                    () -> "preference " + preference + " should abstain against a plan with no vector access");
            assertTrue(costModel.compareVectorIndexEnginePreference(nonVectorPlan, hnswPlan).isEmpty(),
                    () -> "preference " + preference + " should abstain against a plan with no vector access");
            assertTrue(costModel.compareVectorIndexEnginePreference(guardiannPlan, nonVectorPlan).isEmpty(),
                    () -> "preference " + preference + " should abstain against a plan with no vector access");
        }
    }

    /**
     * The same on the preferred engine's own side: two accesses on the preferred engine do not beat one, so the criterion
     * never rewards a plan simply for making more (approximate) vector accesses.
     */
    @Test
    void preferenceDoesNotRewardMoreAccessesOfThePreferredEngine() {
        final var twoGuardiannAccesses =
                planOpsMapOf(vectorIndexPlan("guardiannIndexOne", true), vectorIndexPlan("guardiannIndexTwo", true));
        final var oneGuardiannAccess = planOpsMapOf(vectorIndexPlan("guardiannIndexThree", true));

        final PlanningCostModel costModel = costModel(VectorIndexEnginePreference.PREFER_GUARDIANN);
        assertTrue(costModel.compareVectorIndexEnginePreference(twoGuardiannAccesses, oneGuardiannAccess).isEmpty());
        assertTrue(costModel.compareVectorIndexEnginePreference(oneGuardiannAccess, twoGuardiannAccesses).isEmpty());
    }

    /**
     * Asserts that setting {@code preference} does not change how the cost model orders the two plans, i.e. that the
     * engine preference did not participate in the comparison. Asserting this against the no-preference comparison
     * rather than against a planHash ordering keeps the assertion meaningful whichever way that hash happens to fall.
     *
     * @param a the first plan
     * @param b the second plan
     * @param preference the preference that should make no difference
     */
    private static void assertSameAsWithoutPreference(@Nonnull final RecordQueryIndexPlan a,
                                                      @Nonnull final RecordQueryIndexPlan b,
                                                      @Nonnull final VectorIndexEnginePreference preference) {
        final int withoutPreference = costModel(VectorIndexEnginePreference.NO_PREFERENCE).compare(a, b);
        assertEquals(withoutPreference, costModel(preference).compare(a, b),
                () -> "preference " + preference + " should not have changed the comparison");
    }

    /**
     * A {@code planOpsMap} as {@code compare()} would build it for a member making exactly the given index accesses.
     *
     * @param accesses the index accesses the member makes
     * @return the map of interesting operators
     */
    @Nonnull
    private static Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapOf(@Nonnull final RecordQueryIndexPlan... accesses) {
        return ImmutableMap.of(RecordQueryPlanWithIndex.class, LinkedIdentitySet.of(accesses));
    }
}
