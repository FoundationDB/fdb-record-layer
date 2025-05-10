/*
 * RewritingRuleSet.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.rules.FinalizeExpressionsRule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class RewritingRuleSet extends CascadesRuleSet {
    private static final Set<ExplorationCascadesRule<? extends RelationalExpression>> EXPLORATION_RULES = ImmutableSet.of(
    );
    private static final Set<CascadesRule<? extends RelationalExpression>> PREORDER_RULES = ImmutableSet.of();

    private static final Set<ImplementationCascadesRule<? extends RelationalExpression>> IMPLEMENTATION_RULES = ImmutableSet.of(
            new FinalizeExpressionsRule()
    );

    private static final Set<CascadesRule<? extends RelationalExpression>> ALL_EXPRESSION_RULES =
            ImmutableSet.<CascadesRule<? extends RelationalExpression>>builder()
                    .addAll(PREORDER_RULES)
                    .addAll(EXPLORATION_RULES)
                    .addAll(IMPLEMENTATION_RULES)
                    .build();

    public static final RewritingRuleSet DEFAULT = new RewritingRuleSet();

    @VisibleForTesting
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    RewritingRuleSet() {
        super(ALL_EXPRESSION_RULES);
    }

    @Nonnull
    public static RewritingRuleSet getDefault() {
        return DEFAULT;
    }
}
