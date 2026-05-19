/*
 * PullUpNullOnEmptyRuleTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEachWithNullOnEmpty;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.GREATER_THAN_HELLO;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;

/**
 * Tests of the {@link PullUpNullOnEmptyRule}.
 */
class PullUpNullOnEmptyRuleTest {
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(new PullUpNullOnEmptyRule(), PlannerPhase.PLANNING);

    @BeforeEach
    void setUp() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    /**
     * Tests the rule against the following (pseudo) query:
     * <pre>{@code
     * SELECT b, c FROM (SELECT a, b, c FROM T WHERE b > 'hello') OR ELSE NULL WHERE a = 42
     * }</pre>
     * <p>The rule splits the outer select into a lower select (over a normal quantifier) and an upper select (over the
     * null-on-empty quantifier). After the rewrite, the predicate {@code a = 42} must be attached to both the lower
     * select and the upper select.
     */
    @Test
    void pullUpReappliesPredicatesAboveNullOnEmpty() {
        final Quantifier baseQun = baseT();
        final SelectExpression innerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));
        final Quantifier noeQun = forEachWithNullOnEmpty(innerSelect);
        final SelectExpression outer = selectWithPredicates(noeQun,
                ImmutableList.of("b", "c"),
                fieldPredicate(noeQun, "a", EQUALS_42));

        // Build the expected pulled-up structure. The lower select has a normal ForEach that re-uses the alias of the
        // original null-on-empty quantifier (so that the predicate, which references that alias, continues to resolve).
        // The upper select has a null-on-empty quantifier (cloned from the original) and *also* carries the predicate.
        final Quantifier baseQun2 = baseT();
        final SelectExpression innerSelect2 = selectWithPredicates(baseQun2,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun2, "b", GREATER_THAN_HELLO));
        final Quantifier.ForEach expectedLowerQun = Quantifier.forEachBuilder()
                .withAlias(noeQun.getAlias())
                .build(Reference.initialOf(innerSelect2));
        final SelectExpression expectedLower = GraphExpansion.builder()
                .addQuantifier(expectedLowerQun)
                .addPredicate(fieldPredicate(expectedLowerQun, "a", EQUALS_42))
                .build()
                .buildSimpleSelectOverQuantifier(expectedLowerQun);
        final Quantifier.ForEach expectedTopQun = Quantifier.forEachBuilder()
                .from((Quantifier.ForEach)noeQun)
                .build(Reference.initialOf(expectedLower));
        final SelectExpression expectedTop = GraphExpansion.builder()
                .addQuantifier(expectedTopQun)
                .addPredicate(fieldPredicate(expectedTopQun, "a", EQUALS_42))
                .build()
                .buildSelectWithResultValue(outer.getResultValue());

        testHelper.assertYields(outer, expectedTop);
    }
}
