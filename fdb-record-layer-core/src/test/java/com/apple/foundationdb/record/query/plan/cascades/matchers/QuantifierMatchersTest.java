/*
 * QuantifierMatchersTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.matchers;

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.plainForEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.plainForEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.withNullOnEmpty;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the quantifier matchers in {@link QuantifierMatchers}.
 */
class QuantifierMatchersTest {
    @Nonnull
    private final Reference scanReference = Reference.plannedOf(new RecordQueryScanPlan(ScanComparisons.EMPTY, false));
    @Nonnull
    private final Quantifier.ForEach plainQuantifier = Quantifier.forEach(scanReference);
    @Nonnull
    private final Quantifier.ForEach nullOnEmptyQuantifier = Quantifier.forEachWithNullOnEmpty(scanReference);

    @Nonnull
    private static Optional<PlannerBindings> bindMatches(@Nonnull final BindingMatcher<?> matcher,
                                                         @Nonnull final Quantifier quantifier) {
        return matcher.bindMatches(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                PlannerBindings.empty(), quantifier).findFirst();
    }

    /**
     * Tests that a for-each quantifier is plain exactly if it does not have null-on-empty semantics.
     */
    @Test
    void testIsPlain() {
        assertThat(plainQuantifier.isPlain()).isTrue();
        assertThat(nullOnEmptyQuantifier.isPlain()).isFalse();
    }

    /**
     * Tests that the {@code forEachQuantifier*()} matchers match both plain and null-on-empty quantifiers.
     */
    @Test
    void testForEachQuantifier() {
        for (final Quantifier quantifier : new Quantifier[] {plainQuantifier, nullOnEmptyQuantifier}) {
            assertThat(bindMatches(forEachQuantifier(), quantifier)).isPresent();
            assertThat(bindMatches(forEachQuantifier(RecordQueryPlanMatchers.scanPlan()), quantifier)).isPresent();
            assertThat(bindMatches(forEachQuantifierOverRef(ReferenceMatchers.anyRef()), quantifier)).isPresent();
        }
    }

    /**
     * Tests that the {@code plainForEachQuantifier*()} matchers match a plain quantifier, but not a null-on-empty one.
     */
    @Test
    void testPlainForEachQuantifier1() {
        final BindingMatcher<?> overMembers = plainForEachQuantifier(RecordQueryPlanMatchers.scanPlan());
        assertThat(bindMatches(overMembers, plainQuantifier)).isPresent();
        assertThat(bindMatches(overMembers, nullOnEmptyQuantifier)).isEmpty();

        final BindingMatcher<?> overRef = plainForEachQuantifierOverRef(ReferenceMatchers.anyRef());
        assertThat(bindMatches(overRef, plainQuantifier)).isPresent();
        assertThat(bindMatches(overRef, nullOnEmptyQuantifier)).isEmpty();
    }

    /**
     * Tests that {@code plainForEachQuantifierOverRef()} binds its downstream matcher to the reference of the
     * quantifier.
     */
    @Test
    void testPlainForEachQuantifier2() {
        final BindingMatcher<Reference> referenceMatcher = ReferenceMatchers.anyRef();
        final Optional<PlannerBindings> bindings =
                bindMatches(plainForEachQuantifierOverRef(referenceMatcher), plainQuantifier);
        assertThat(bindings).isPresent();
        assertThat(bindings.get().get(referenceMatcher)).isSameAs(scanReference);
    }

    /**
     * Tests that {@code plainForEachQuantifier()} still requires its downstream matcher to match.
     */
    @Test
    void testPlainForEachQuantifier3() {
        assertThat(bindMatches(plainForEachQuantifier(RecordQueryPlanMatchers.indexPlan()), plainQuantifier)).isEmpty();
    }

    /**
     * Tests that the plain matchers do not match quantifiers other than for-each quantifiers.
     */
    @Test
    void testPlainForEachQuantifier4() {
        final BindingMatcher<?> matcher = plainForEachQuantifierOverRef(ReferenceMatchers.anyRef());
        assertThat(bindMatches(matcher, Quantifier.existential(scanReference))).isEmpty();
        assertThat(bindMatches(matcher, Quantifier.physical(scanReference))).isEmpty();
    }

    /**
     * Tests that {@code withNullOnEmpty()} constrains a for-each matcher to quantifiers with the given null-on-empty
     * flag.
     */
    @Test
    void testWithNullOnEmpty() {
        final BindingMatcher<?> withNullOnEmpty = withNullOnEmpty(true, forEachQuantifier());
        assertThat(bindMatches(withNullOnEmpty, nullOnEmptyQuantifier)).isPresent();
        assertThat(bindMatches(withNullOnEmpty, plainQuantifier)).isEmpty();

        final BindingMatcher<?> withoutNullOnEmpty = withNullOnEmpty(false, forEachQuantifier());
        assertThat(bindMatches(withoutNullOnEmpty, plainQuantifier)).isPresent();
        assertThat(bindMatches(withoutNullOnEmpty, nullOnEmptyQuantifier)).isEmpty();
    }
}
