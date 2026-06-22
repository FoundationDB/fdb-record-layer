/*
 * AnyOfMatcherTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyOfMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnyOfMatcherTest {

    @Test
    public void explainMatcherShowsAnyOfWithDownstreamExplanations() {
        final BindingMatcher<Integer> matcher =
                AnyOfMatcher.matchingAnyOf(Integer.class, ImmutableList.of(PrimitiveMatchers.equalsObject(42)));

        final String explanation = matcher.explainMatcher(Integer.class, "x", "");

        assertEquals("""
                any of {
                    match x { case test(42) => success }
                }""", explanation);
    }

    @Test
    public void bindMatchesSafelyMatchesWhenAnyDownstreamMatches() {
        final BindingMatcher<Integer> matchFive = PrimitiveMatchers.equalsObject(5);
        final BindingMatcher<Integer> matchSeven = PrimitiveMatchers.equalsObject(7);
        final AnyOfMatcher<Integer> matcher =
                AnyOfMatcher.matchingAnyOf(Integer.class, ImmutableList.of(matchFive, matchSeven));

        final RecordQueryPlannerConfiguration config = RecordQueryPlannerConfiguration.defaultPlannerConfiguration();

        // matches first downstream
        Optional<PlannerBindings> bindingsForFive =
                matcher.bindMatches(config, PlannerBindings.empty(), 5).findFirst();
        assertTrue(bindingsForFive.isPresent());
        assertEquals(5, (int) bindingsForFive.get().get(matchFive));

        // matches second downstream
        Optional<PlannerBindings> bindingsForSeven =
                matcher.bindMatches(config, PlannerBindings.empty(), 7).findFirst();
        assertTrue(bindingsForSeven.isPresent());
        assertEquals(7, (int) bindingsForSeven.get().get(matchSeven));

        // matches neither downstream
        Optional<PlannerBindings> bindingsForThree =
                matcher.bindMatches(config, PlannerBindings.empty(), 3).findFirst();
        assertFalse(bindingsForThree.isPresent());
    }
}
