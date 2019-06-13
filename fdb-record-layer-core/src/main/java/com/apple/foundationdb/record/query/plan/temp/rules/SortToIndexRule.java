/*
 * SortToIndexRule.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;

/**
 * A rule for implementing a {@link LogicalSortExpression} as a scan of an appropriately-ordered index.
 * There are a few things to note about how this rule currently works:
 * <ul>
 *     <li>
 *         It relies on {@link KeyExpression#isPrefixKey(KeyExpression)} to do the "heavy lifting" of determining
 *         whether or not a sort can be implemented using an index, which currently involves calling
 *         {@link ExpressionRef#get()}. This obviously does not work if the {@code ExpressionRef} is not gettable
 *         (for example, if it were a group).
 *     </li>
 *     <li>
 *         It will plan the sort using the first index with an appropriate ordering.
 *     </li>
 * </ul>
 * Ths first of these details will definitely change as the planner improves.
 */
@API(API.Status.EXPERIMENTAL)
public class SortToIndexRule extends PlannerRule<LogicalSortExpression> {
    private static final ExpressionMatcher<FullUnorderedScanExpression> innerMatcher = TypeMatcher.of(FullUnorderedScanExpression.class);
    private static final ExpressionMatcher<LogicalSortExpression> root = TypeMatcher.of(LogicalSortExpression.class, innerMatcher);

    public SortToIndexRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalSortExpression logicalSort = call.get(root);
        final KeyExpressionComparisons requestedSort = logicalSort.getSort();
        final boolean reverse = call.get(root).isReverse();

        ChangesMade madeChanges = ChangesMade.NO_CHANGE;
        for (IndexEntrySource indexEntrySource : call.getContext().getIndexEntrySources()) {
            final KeyExpressionComparisons sortExpression = indexEntrySource.getEmptyComparisons();
            if (sortExpression.supportsSortOrder(requestedSort)) {
                call.yield(call.ref(new IndexEntrySourceScanExpression(indexEntrySource, IndexScanType.BY_VALUE,
                        sortExpression, reverse)));
                madeChanges = ChangesMade.MADE_CHANGES;
            }
        }
        return madeChanges;
    }
}
