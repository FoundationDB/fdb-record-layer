/*
 * VectorIndexPlanTest.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PPlanReference;
import com.apple.foundationdb.record.planprotos.PVectorIndexPlan;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexEngineKind;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.IndexTraversalKind;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link VectorIndexPlan}, which, as nothing creates it yet, can only be obtained by deserializing one.
 */
class VectorIndexPlanTest {
    @ParameterizedTest
    @CsvSource({"HNSW, HNSW", "GUARDIANN, GUARDIANN"})
    void indexTraversalKindFollowsEngine(@Nonnull final VectorIndexEngineKind engine,
                                         @Nonnull final IndexTraversalKind expectedKind) {
        final VectorIndexPlan plan = vectorIndexPlan(engine);
        assertThat(plan.getEngineKind()).isEqualTo(engine);
        assertThat(plan.getIndexTraversalKind()).isEqualTo(expectedKind);
    }

    @ParameterizedTest
    @EnumSource(VectorIndexEngineKind.class)
    void serializationRoundTripPreservesEngine(@Nonnull final VectorIndexEngineKind engine) throws Exception {
        final VectorIndexPlan plan = vectorIndexPlan(engine);

        PlanSerializationContext serializationContext = PlanSerializationContext.newForCurrentMode();
        final PPlanReference proto = serializationContext.toPlanReferenceProto(plan);
        final PPlanReference parsedProto = PPlanReference.parseFrom(proto.toByteArray());
        serializationContext = PlanSerializationContext.newForCurrentMode();
        final RecordQueryPlan parsedPlan = serializationContext.fromPlanReferenceProto(parsedProto);

        assertThat(parsedPlan).isInstanceOf(VectorIndexPlan.class);
        assertThat(((VectorIndexPlan)parsedPlan).getEngineKind()).isEqualTo(engine);
        assertThat(plan.semanticEquals(parsedPlan)).isTrue();
    }

    @Test
    void deserializationWithoutEngineFails() {
        final PVectorIndexPlan proto = PVectorIndexPlan.newBuilder()
                .setSuper(indexPlan().toRecordQueryIndexPlanProto(PlanSerializationContext.newForCurrentMode()))
                .build();
        assertThatThrownBy(() -> VectorIndexPlan.fromProto(PlanSerializationContext.newForCurrentMode(), proto))
                .isInstanceOf(VerifyException.class);
    }

    @Test
    void plansWithDifferentEnginesAreNotEqual() {
        assertThat(vectorIndexPlan(VectorIndexEngineKind.HNSW)
                .semanticEquals(vectorIndexPlan(VectorIndexEngineKind.GUARDIANN))).isFalse();
    }

    @Test
    void plansAreNotEqualToTheirIndexPlanSuper() {
        assertThat(vectorIndexPlan(VectorIndexEngineKind.HNSW).semanticEquals(indexPlan())).isFalse();
    }

    /**
     * A plan deserialized from a newer writer can still be copied by this version, so every copy has to keep the engine
     * kind rather than degrade to a plain {@link RecordQueryIndexPlan}.
     */
    @Test
    void copyingAPlanKeepsTheEngineKind() {
        final VectorIndexPlan plan = vectorIndexPlan(VectorIndexEngineKind.GUARDIANN);

        assertThat(plan.strictlySorted(Memoizer.noMemoization(PlannerStage.PLANNED)))
                .isInstanceOf(VectorIndexPlan.class)
                .extracting(VectorIndexPlan::getEngineKind)
                .isEqualTo(VectorIndexEngineKind.GUARDIANN);
        assertThat(plan.strictlySorted(Memoizer.noMemoization(PlannerStage.PLANNED)).isStrictlySorted()).isTrue();

        assertThat(plan.minimize(ImmutableList.of()))
                .isInstanceOf(VectorIndexPlan.class)
                .extracting(VectorIndexPlan::getEngineKind)
                .isEqualTo(VectorIndexEngineKind.GUARDIANN);

        assertThat(plan.withIndexScanParameters(vectorScan()))
                .isInstanceOf(VectorIndexPlan.class)
                .extracting(VectorIndexPlan::getEngineKind)
                .isEqualTo(VectorIndexEngineKind.GUARDIANN);
    }

    @Nonnull
    private static VectorIndexPlan vectorIndexPlan(@Nonnull final VectorIndexEngineKind engine) {
        final PlanSerializationContext serializationContext = PlanSerializationContext.newForCurrentMode();
        final PVectorIndexPlan proto = PVectorIndexPlan.newBuilder()
                .setSuper(indexPlan().toRecordQueryIndexPlanProto(serializationContext))
                .setEngineKind(engine.toProto())
                .build();
        return VectorIndexPlan.fromProto(serializationContext, proto);
    }

    @Nonnull
    private static RecordQueryIndexPlan indexPlan() {
        return new RecordQueryIndexPlan("a_vector_index",
                null,
                vectorScan(),
                IndexFetchMethod.SCAN_AND_FETCH,
                FetchIndexRecords.PRIMARY_KEY,
                false,
                false,
                Optional.empty(),
                new Type.Any(),
                QueryPlanConstraint.noConstraint());
    }

    @Nonnull
    private static IndexScanParameters vectorScan() {
        final int numDimensions = 128;
        final Comparisons.DistanceRankValueComparison distanceRankValueComparison =
                new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 64, numDimensions),
                                new DoubleRealVector(new double[numDimensions])),
                        new LiteralValue<>(10), null, null);
        return VectorIndexScanComparisons.byDistance(ScanComparisons.EMPTY, distanceRankValueComparison);
    }
}
