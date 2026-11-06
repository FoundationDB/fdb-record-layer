/*
 * RecordQueryScanPlanTest.java
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

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RecordQueryScanPlan}.
 */
class RecordQueryScanPlanTest {
    private static final Type.Record FLOWED_TYPE = Type.Record.fromFields(List.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"))));

    private static RecordQueryScanPlan scanPlanWithEqualityOn(final Value comparand) {
        final var scanComparisons = new ScanComparisons(
                List.of(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, comparand)),
                Set.of());
        return new RecordQueryScanPlan(
                ImmutableSet.of("R"), FLOWED_TYPE, null, scanComparisons, false, false);
    }

    @Test
    void translateCorrelationsRebasesComparisonsToTargetAlias() {
        final var source = CorrelationIdentifier.of("source");
        final var target = CorrelationIdentifier.of("target");
        final var sourceScanPlan = scanPlanWithEqualityOn(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(source, FLOWED_TYPE), "a"));
        final var targetScanPlan = scanPlanWithEqualityOn(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(target, FLOWED_TYPE), "a"));

        final var translatedPlan = sourceScanPlan.translateCorrelations(
                TranslationMap.ofAliases(source, target), false, List.of());

        assertThat(sourceScanPlan.getCorrelatedTo()).containsExactly(source);
        assertThat(translatedPlan.getCorrelatedTo()).containsExactly(target);
        assertThat(translatedPlan.getScanComparisons()).isEqualTo(targetScanPlan.getScanComparisons());
    }

    @Test
    void getDynamicTypesReturnsResultValueAndComparisonTypes() {
        final var comparand = RecordConstructorValue.ofUnnamed(List.of(LiteralValue.ofScalar(1L)));
        final var scanPlan = scanPlanWithEqualityOn(comparand);

        assertThat(scanPlan.getDynamicTypes()).containsExactlyInAnyOrder(FLOWED_TYPE, comparand.getResultType());
    }
}
