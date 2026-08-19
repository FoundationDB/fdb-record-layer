/*
 * SelectMergeablePropertyTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.logicalFilterExpressionWithPredicates;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.TYPE_T;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.fuseExpression;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.fuseQun;
import static com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SelectMergeableProperty}.
 */
class SelectMergeablePropertyTest {

    private static final SelectMergeableProperty PROPERTY = SelectMergeableProperty.selectMergeable();

    @Test
    void evaluateReturnsTrueForSelectExpression() {
        final var base = baseT();
        final var select = selectWithPredicates(base, fieldPredicate(base, "a", EQUALS_42));
        assertThat(PROPERTY.evaluate(select)).isTrue();
    }

    @Test
    void evaluateReturnsTrueForSelectExpressionWithoutPredicates() {
        final var select = selectWithPredicates(baseT());
        assertThat(PROPERTY.evaluate(select)).isTrue();
    }

    @Test
    void evaluateReturnsTrueForLogicalFilterExpression() {
        final var base = baseT();
        final var filter = logicalFilterExpressionWithPredicates(base, fieldPredicate(base, "a", EQUALS_42));
        assertThat(PROPERTY.evaluate(filter)).isTrue();
    }

    @Test
    void evaluateReturnsFalseForLogicalTypeFilterExpression() {
        final var typeFilter = of(ImmutableSet.of("T"), fuseQun(), TYPE_T);
        assertThat(PROPERTY.evaluate(typeFilter)).isFalse();
    }

    @Test
    void evaluateReturnsFalseForLeafExpression() {
        assertThat(PROPERTY.evaluate(fuseExpression())).isFalse();
    }

    @Test
    void evaluateOnReferenceTrueForSelectExpression() {
        final var base = baseT();
        final var select = selectWithPredicates(base);
        final var ref = Reference.initialOf(select);
        assertThat(PROPERTY.evaluate(ref)).isTrue();
    }

    @Test
    void evaluateOnReferenceFalseForNonMergeableExpression() {
        final var typeFilter = of(ImmutableSet.of("T"), fuseQun(), TYPE_T);
        final var ref = Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableSet.of(typeFilter));
        assertThat(PROPERTY.evaluate(ref)).isFalse();
    }

    @Test
    void toStringReturnsSimpleClassName() {
        assertThat(PROPERTY.toString()).isEqualTo("SelectMergeableProperty");
    }
}
