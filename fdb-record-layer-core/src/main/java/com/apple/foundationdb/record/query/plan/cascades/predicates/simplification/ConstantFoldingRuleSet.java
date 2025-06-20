/*
 * ConstantFoldingRuleSet.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for folding constant {@link QueryPredicate}s.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class ConstantFoldingRuleSet extends DefaultQueryPredicateRuleSet {
    protected static final QueryPredicateSimplificationRule<? extends QueryPredicate> valuePredicateSimplificationRule = new ValuePredicateSimplificationRule();
    protected static final QueryPredicateSimplificationRule<? extends QueryPredicate> constantFoldingBooleanLiteralsRule = new ConstantFoldingValuePredicateRule();
    protected static final QueryPredicateSimplificationRule<? extends QueryPredicate> constantFoldingBooleanPredicateWithRangesRule = new ConstantFoldingPredicateWithRangesRule();
    protected static final QueryPredicateSimplificationRule<? extends QueryPredicate> constantFoldingImpossiblePredicateWithRangesRule = new ConstantFoldingMultiConstraintPredicateRule();

    protected static final Set<QueryPredicateSimplificationRule<? extends QueryPredicate>> SIMPLIFICATION_WITH_CONSTANT_FOLDING_RULES =
            ImmutableSet.<QueryPredicateSimplificationRule<? extends QueryPredicate>>builder()
                    .addAll(SIMPLIFICATION_RULES)
                    .add(valuePredicateSimplificationRule)
                    .add(constantFoldingBooleanLiteralsRule)
                    .add(constantFoldingBooleanPredicateWithRangesRule)
                    .add(constantFoldingImpossiblePredicateWithRangesRule)
                    .build();

    protected static final SetMultimap<QueryPredicateSimplificationRule<? extends QueryPredicate>, QueryPredicateSimplificationRule<? extends QueryPredicate>> SIMPLIFICATION_WITH_CONSTANT_FOLDING_DEPENDS_ON;

    static {
        final var simplificationDependsOnBuilder =
                ImmutableSetMultimap.<QueryPredicateSimplificationRule<? extends QueryPredicate>, QueryPredicateSimplificationRule<? extends QueryPredicate>>builder();
        simplificationDependsOnBuilder.putAll(SIMPLIFICATION_DEPENDS_ON);

        SIMPLIFICATION_RULES.forEach(existingRule -> simplificationDependsOnBuilder.put(existingRule, valuePredicateSimplificationRule));
        SIMPLIFICATION_RULES.forEach(existingRule -> simplificationDependsOnBuilder.put(existingRule, constantFoldingBooleanLiteralsRule));
        SIMPLIFICATION_RULES.forEach(existingRule -> simplificationDependsOnBuilder.put(existingRule, constantFoldingBooleanPredicateWithRangesRule));
        SIMPLIFICATION_RULES.forEach(existingRule -> simplificationDependsOnBuilder.put(existingRule, constantFoldingImpossiblePredicateWithRangesRule));
        SIMPLIFICATION_WITH_CONSTANT_FOLDING_DEPENDS_ON = simplificationDependsOnBuilder.build();
    }

    private ConstantFoldingRuleSet() {
        this(SIMPLIFICATION_WITH_CONSTANT_FOLDING_RULES, SIMPLIFICATION_WITH_CONSTANT_FOLDING_DEPENDS_ON);
    }

    protected ConstantFoldingRuleSet(@Nonnull final Set<? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>> abstractQueryPredicateRules,
                                     @Nonnull final SetMultimap<? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>, ? extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, ? extends QueryPredicate>> dependsOn) {
        super(abstractQueryPredicateRules, dependsOn);
    }

    @Nonnull
    public static ConstantFoldingRuleSet ofSimplificationRules() {
        return new ConstantFoldingRuleSet();
    }
}
