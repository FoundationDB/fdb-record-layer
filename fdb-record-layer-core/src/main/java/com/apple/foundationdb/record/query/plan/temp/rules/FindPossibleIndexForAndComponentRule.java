/*
 * FindPossibleIndexForAndComponentRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * A rule that finds all indexes that could implement one of the {@link ComponentWithComparison} conjuncts of an AND
 * filter, leaving all the other filters (of any type, including other fields) as a residual filter.
 */
@API(API.Status.EXPERIMENTAL)
public class FindPossibleIndexForAndComponentRule extends PlannerRule<LogicalFilterExpression> {
    private static ExpressionMatcher<ComponentWithComparison> fieldMatcher = TypeMatcher.of(FieldWithComparison.class);
    private static ReferenceMatcher<QueryComponent> residualFieldsMatcher = ReferenceMatcher.anyRef();
    private static ExpressionMatcher<AndComponent> andFilterMatcher = TypeMatcher.of(AndComponent.class,
            AnyChildWithRestMatcher.anyMatchingWithRest(fieldMatcher, residualFieldsMatcher));
    private static ExpressionMatcher<FullUnorderedScanExpression> scanMatcher = TypeMatcher.of(FullUnorderedScanExpression.class);
    private static ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class,
            andFilterMatcher, scanMatcher);

    public FindPossibleIndexForAndComponentRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        ComponentWithComparison field = call.getBindings().get(fieldMatcher);
        ChangesMade madeChanges = ChangesMade.NO_CHANGE;
        for (IndexEntrySource indexEntrySource : call.getContext().getIndexEntrySources()) {
            final KeyExpressionComparisons keyComparisons = indexEntrySource.getEmptyComparisons();
            final Optional<KeyExpressionComparisons> matchedKeyComparisons = keyComparisons.matchWith(field);
            if (matchedKeyComparisons.isPresent()) {
                final List<ExpressionRef<QueryComponent>> otherFields = call.getBindings().getAll(residualFieldsMatcher);
                final ExpressionRef<QueryComponent> residualFilter;
                if (otherFields.size() == 1) {
                    residualFilter = otherFields.get(0);
                } else {
                    residualFilter = call.ref(new AndComponent(otherFields));
                }
                call.yield(call.ref(new LogicalFilterExpression(residualFilter,
                        call.ref(new IndexEntrySourceScanExpression(indexEntrySource, IndexScanType.BY_VALUE,
                                matchedKeyComparisons.get(), false)))));
                madeChanges = ChangesMade.MADE_CHANGES;
            }
        }

        return madeChanges;
    }
}
