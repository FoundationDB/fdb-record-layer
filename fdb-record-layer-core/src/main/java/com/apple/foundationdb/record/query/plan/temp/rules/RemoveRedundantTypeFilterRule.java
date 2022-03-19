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
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers;
import com.apple.foundationdb.record.query.plan.temp.properties.RecordTypesProperty;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.logicalTypeFilterExpression;

/**
 * A rule that eliminates logical type filters that are completely redundant; that is, when the child of the logical
 * type filter is guaranteed to return records of types included in the filter.
 */
@API(API.Status.EXPERIMENTAL)
public class RemoveRedundantTypeFilterRule extends PlannerRule<LogicalTypeFilterExpression> {
    private static final BindingMatcher<Quantifier.ForEach> qunMatcher = QuantifierMatchers.forEachQuantifier();
    private static final BindingMatcher<LogicalTypeFilterExpression> root = logicalTypeFilterExpression(exactly(qunMatcher));

    public RemoveRedundantTypeFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var typeFilter = call.get(root);
        final var qun = call.get(qunMatcher);

        // TODO add overload
        final var childRecordTypes = RecordTypesProperty.evaluate(call.getContext(), call.getAliasResolver(), qun.getRangesOver());
        final var filterRecordTypes = Sets.newHashSet(typeFilter.getRecordTypes());
        if (filterRecordTypes.containsAll(childRecordTypes)) {
            // type filter is completely redundant, so remove it entirely
            call.yield(qun.getRangesOver());
        } else {
            // otherwise, keep a logical filter on record types which the quantifier might produce and are included in the filter
            final var unsatisfiedTypeFilters = Sets.intersection(childRecordTypes, filterRecordTypes);
            if (!unsatisfiedTypeFilters.equals(filterRecordTypes)) {
                final var planContext = call.getContext();
                final var metaData = planContext.getMetaData();
                // there were some unnecessary filters, so remove them
                call.yield(
                        GroupExpressionRef.of(
                                new LogicalTypeFilterExpression(
                                        unsatisfiedTypeFilters,
                                        qun,
                                        Type.Record.fromFieldDescriptorsMap(metaData.getFieldDescriptorMapFromNames(unsatisfiedTypeFilters)))));
            } // otherwise, nothing changes
        }
    }
}
