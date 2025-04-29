/*
 * CascadesRuleSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.AbstractRuleSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class CascadesRuleSet extends AbstractRuleSet<RelationalExpression, CascadesRuleCall, RelationalExpression> {
    @VisibleForTesting
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    CascadesRuleSet(@Nonnull Set<CascadesRule<? extends RelationalExpression>> rules) {
        super(rules, ImmutableSetMultimap.of());
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public Stream<? extends CascadesRule<? extends RelationalExpression>> getRules(@Nonnull final RelationalExpression value) {
        return (Stream<? extends CascadesRule<? extends RelationalExpression>>)super.getRules(value);
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public Stream<? extends CascadesRule<? extends RelationalExpression>> getRules(@Nonnull final RelationalExpression value, @Nonnull final Predicate<PlannerRule<RelationalExpression, CascadesRuleCall, ? extends RelationalExpression>> plannerRulePredicate) {
        return (Stream<? extends CascadesRule<? extends RelationalExpression>>)super.getRules(value, plannerRulePredicate);
    }

    @Nonnull
    public Stream<CascadesRule<? extends PartialMatch>> getPartialMatchRules() {
        return getPartialMatchRules(ignored -> true);
    }

    @Nonnull
    public Stream<CascadesRule<? extends PartialMatch>> getPartialMatchRules(@Nonnull final Predicate<CascadesRule<? extends PartialMatch>> rulePredicate) {
        return Stream.empty();
    }


    @Nonnull
    public Stream<CascadesRule<? extends MatchPartition>> getMatchPartitionRules() {
        return getMatchPartitionRules(ignored -> true);
    }

    @Nonnull
    public Stream<CascadesRule<? extends MatchPartition>> getMatchPartitionRules(@Nonnull final Predicate<CascadesRule<? extends MatchPartition>> rulePredicate) {
        return Stream.empty();
    }
}
