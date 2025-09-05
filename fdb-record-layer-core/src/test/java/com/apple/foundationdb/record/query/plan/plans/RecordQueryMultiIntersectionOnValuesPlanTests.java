/*
 * RecordQueryMultiIntersectionOnValuesPlanTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

public class RecordQueryMultiIntersectionOnValuesPlanTests {
    @Test
    public void testHeuristicPlannerEntryPoints() {
        final var memoizer = Memoizer.noMemoization(PlannerStage.PLANNED);
        final var scanA = memoizer.memoizePlan(scan());
        final var qunA = Quantifier.physical(scanA, CorrelationIdentifier.of("a"));
        final var scanB = memoizer.memoizePlan(scan());
        final var qunB = Quantifier.physical(scanB, CorrelationIdentifier.of("b"));

        final var intersectionPLan =
                RecordQueryMultiIntersectionOnValuesPlan.intersection(ImmutableList.of(qunA, qunB),
                        ImmutableList.of(),
                        QuantifiedObjectValue.of(qunA),
                        false);

        Assertions.assertThat(intersectionPLan)
                .satisfies(plan ->
                        Assertions.assertThatThrownBy(() -> plan.getRequiredValues(CorrelationIdentifier.uniqueId(), new Type.Any()))
                                .isInstanceOf(UnsupportedOperationException.class))
                .satisfies(plan ->
                        Assertions.assertThatThrownBy(plan::getRequiredFields)
                                .isInstanceOf(UnsupportedOperationException.class))
                .satisfies(plan -> {
                    final var scanANew = memoizer.memoizePlan(scan());
                    final var scanBNew = memoizer.memoizePlan(scan());

                    Assertions.assertThat(plan.withChildrenReferences(ImmutableList.of(scanANew, scanBNew)))
                            .isEqualTo(intersectionPLan);
                })
                .satisfies(plan ->
                        Assertions.assertThatThrownBy(() -> plan.strictlySorted(memoizer))
                                .isInstanceOf(UnsupportedOperationException.class));
    }

    @Nonnull
    private static RecordQueryScanPlan scan() {
        return new RecordQueryScanPlan(ImmutableSet.of("someType"),
                someRecordType(),
                EmptyKeyExpression.EMPTY,
                ScanComparisons.EMPTY,
                false,
                false,
                Optional.empty());
    }

    @Nonnull
    private static Type.Record someRecordType() {
        return RuleTestHelper.TYPE_S;
    }
}
