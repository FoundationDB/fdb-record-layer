/*
 * RemoveRedundantTypeFilterRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.RecordTypesProperty;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A rule that eliminates logical type filters that are completely redundant; that is, when the child of the logical
 * type filter is guaranteed to return records of types included in the filter.
 */
@API(API.Status.EXPERIMENTAL)
public class RemoveRedundantTypeFilterRule extends PlannerRule<LogicalTypeFilterExpression> {
    private static ExpressionMatcher<Quantifier.ForEach> qunMatcher = QuantifierMatcher.forEach(ReferenceMatcher.anyRef());
    private static ExpressionMatcher<LogicalTypeFilterExpression> root =
            TypeMatcher.of(LogicalTypeFilterExpression.class, qunMatcher);

    public RemoveRedundantTypeFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalTypeFilterExpression typeFilter = call.get(root);
        final Quantifier.ForEach qun = call.get(qunMatcher);

        // TODO add overload
        final Set<String> childRecordTypes = RecordTypesProperty.evaluate(call.getContext(), call.getAliasResolver(), qun.getRangesOver());
        final Set<String> filterRecordTypes = Sets.newHashSet(typeFilter.getRecordTypes());
        if (filterRecordTypes.containsAll(childRecordTypes)) {
            // type filter is completely redundant, so remove it entirely
            call.yield(qun.getRangesOver());
        } else {
            // otherwise, keep a logical filter on record types which the quantifier might produce and are included in the filter
            final Set<String> unsatisfiedTypeFilters = Sets.intersection(childRecordTypes, filterRecordTypes);
            if (!unsatisfiedTypeFilters.equals(filterRecordTypes)) {
                // there were some unnecessary filters, so remove them
                call.yield(GroupExpressionRef.of(new LogicalTypeFilterExpression(unsatisfiedTypeFilters, qun)));
            } // otherwise, nothing changes
        }
    }
}
