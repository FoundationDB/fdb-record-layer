/*
 * RecordQueryIndexPlanPhysicalIndexKindTest.java
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

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.PhysicalIndexKind;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Traversal;
import com.apple.foundationdb.record.query.plan.cascades.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests that a {@link RecordQueryIndexPlan} carries the {@link PhysicalIndexKind} of the access it was created for, that
 * it still carries it after a round trip through serialization — which is the point of holding it on the plan rather than
 * reading it off the match candidate — and that only {@code VC1} accounts for it in the plan hash.
 */
class RecordQueryIndexPlanPhysicalIndexKindTest {
    @Nonnull
    private static Type.Record baseType() {
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("aField"))));
    }

    @Nonnull
    private static RecordQueryIndexPlan valueIndexPlan(@Nonnull final String indexName) {
        final Index index = new Index(indexName, field("aField"), IndexTypes.VALUE);
        final Type.Record baseType = baseType();
        final ValueIndexScanMatchCandidate matchCandidate =
                new ValueIndexScanMatchCandidate(index,
                        Collections.<RecordType>emptyList(),
                        Traversal.withRoot(Reference.empty()),
                        ImmutableList.<CorrelationIdentifier>of(),
                        baseType,
                        CorrelationIdentifier.of("base"),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        field("aField"),
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


    /**
     * The same plan with its carried kind stripped, as a plan written before the kind existed reads back.
     *
     * @param plan the plan to strip
     * @return the same plan, carrying no kind
     */
    @Nonnull
    private static RecordQueryIndexPlan withoutKind(@Nonnull final RecordQueryIndexPlan plan) {
        return RecordQueryIndexPlan.fromProto(PlanSerializationContext.newForCurrentMode(),
                plan.toRecordQueryIndexPlanProto(PlanSerializationContext.newForCurrentMode())
                        .toBuilder()
                        .clearPhysicalIndexKind()
                        .build());
    }

    @Test
    void kindIsTakenFromTheMatchCandidate() {
        assertEquals(PhysicalIndexKind.BTREE, valueIndexPlan("anIndex").getPhysicalIndexKind());
    }

    /**
     * The whole reason the kind sits on the plan: a match candidate is not serialized, so a plan that read its kind from
     * the candidate would lose it here.
     */
    @Test
    void kindSurvivesSerialization() {
        final RecordQueryIndexPlan plan = valueIndexPlan("anIndex");
        final var serializationContext = PlanSerializationContext.newForCurrentMode();
        final RecordQueryIndexPlan deserialized =
                RecordQueryIndexPlan.fromProto(serializationContext,
                        plan.toRecordQueryIndexPlanProto(serializationContext));

        assertEquals(PhysicalIndexKind.BTREE, deserialized.getPhysicalIndexKind(),
                "the kind should survive a round trip even though the match candidate does not");
        assertEquals(Optional.empty(), deserialized.getMatchCandidateMaybe(),
                "sanity: the match candidate itself is still not serialized");
    }

    /**
     * Accounting for the kind changes a plan's hash, so it may only be done from {@code VC1} on. {@code VC0} is
     * {@link PlanHashable#CURRENT_FOR_CONTINUATION} and its hashes are load-bearing for continuations.
     */
    @Test
    void onlyVc1AccountsForTheKind() {
        final RecordQueryIndexPlan plan = valueIndexPlan("anIndex");
        final RecordQueryIndexPlan sameButUnknownKind = withoutKind(plan);

        assertEquals(PhysicalIndexKind.UNKNOWN, sameButUnknownKind.getPhysicalIndexKind(),
                "a plan carrying no kind should read back as UNKNOWN");
        assertEquals(plan.planHash(PlanHashable.PlanHashMode.VC0),
                sameButUnknownKind.planHash(PlanHashable.PlanHashMode.VC0),
                "VC0 must not account for the kind");
        assertNotEquals(plan.planHash(PlanHashable.PlanHashMode.VC1),
                sameButUnknownKind.planHash(PlanHashable.PlanHashMode.VC1),
                "VC1 must account for the kind");
    }

    /**
     * A plan with several children reports the combination of their kinds, so a union of two value index scans is still
     * {@code BTREE} while a union with something unclassified is {@code MIXED}.
     */
    @Test
    void unionCombinesItsChildrenKinds() {
        assertEquals(PhysicalIndexKind.BTREE,
                union(valueIndexPlan("oneIndex"), valueIndexPlan("anotherIndex")).getPhysicalIndexKind());
        assertEquals(PhysicalIndexKind.MIXED,
                union(valueIndexPlan("oneIndex"), withoutKind(valueIndexPlan("anotherIndex"))).getPhysicalIndexKind());
    }

    /**
     * Only the index scan itself folds the kind into its hash; a plan above it picks that up through its children's
     * hashes, so the {@code VC0}/{@code VC1} split holds for a whole plan tree and not just for the scan.
     */
    @Test
    void theKindReachesAParentPlansVc1Hash() {
        final RecordQueryIndexPlan child = valueIndexPlan("anIndex");
        final RecordQueryPlan withKind = union(child, valueIndexPlan("anotherIndex"));
        final RecordQueryPlan withoutChildKind = union(withoutKind(child), valueIndexPlan("anotherIndex"));

        assertEquals(withKind.planHash(PlanHashable.PlanHashMode.VC0),
                withoutChildKind.planHash(PlanHashable.PlanHashMode.VC0),
                "VC0 must not account for a child's kind either");
        assertNotEquals(withKind.planHash(PlanHashable.PlanHashMode.VC1),
                withoutChildKind.planHash(PlanHashable.PlanHashMode.VC1),
                "VC1 should account for a child's kind through the child's own hash");
    }

    @Nonnull
    private static RecordQueryPlan union(@Nonnull final RecordQueryPlan left, @Nonnull final RecordQueryPlan right) {
        return RecordQueryUnionPlan.from(left, right, field("aField"), false);
    }

}
