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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalGroupRefHolder;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AllChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A rule that finds all indexes that could implement one of the {@link FieldWithComparison} conjuncts of an AND filter
 * and produces a {@link RelationalGroupRefHolder} containing a plan that scans each of those indexes. These can then
 * be planned individually and then compared using the {@link PickFromPossibilitiesRule}.
 *
 * <p>
 * Currently, this rule is pretty rough. Among other things, it does not have proper support for using the primary key
 * index and it pushes the entire filter into each of the members of the
 * {@link com.apple.foundationdb.record.query.plan.temp.FixedCollectionExpressionRef}, rather than removing the
 * comparison that is already planned. This rule will probably change substantially as the planner improves.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class FindPossibleIndexForAndComponentRule extends PlannerRule<LogicalFilterExpression> {
    private static ExpressionMatcher<FieldWithComparison> fieldMatcher = TypeMatcher.of(FieldWithComparison.class);
    private static ExpressionMatcher<AndComponent> andFilterMatcher = TypeMatcher.of(AndComponent.class,
            AllChildrenMatcher.allMatching(fieldMatcher));
    private static ExpressionMatcher<RecordQueryScanPlan> scanMatcher = TypeMatcher.of(RecordQueryScanPlan.class);
    private static ExpressionMatcher<LogicalFilterExpression> root = TypeMatcher.of(LogicalFilterExpression.class,
            andFilterMatcher, scanMatcher);

    public FindPossibleIndexForAndComponentRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        if (!call.get(scanMatcher).hasFullRecordScan()) {
            return ChangesMade.NO_CHANGE; // already has some bounds on the scan
        }

        List<FieldWithComparison> fields = call.getBindings().getAll(fieldMatcher);
        List<RelationalPlannerExpression> possibilities = new ArrayList<>();
        KeyExpressionComparisons keyComparisons;
        Optional<KeyExpressionComparisons> matchedKeyComparisons;

        KeyExpression commonPrimaryKey = call.getContext().getCommonPrimaryKey();
        if (commonPrimaryKey != null) {
            keyComparisons = new KeyExpressionComparisons(commonPrimaryKey);
            for (FieldWithComparison field : fields) {
                matchedKeyComparisons = keyComparisons.matchWith(field);
                if (matchedKeyComparisons.isPresent()) {
                    // TODO improve with a logical version of a RecordQueryScanPlan that holds a KeyExpressionComparisons.
                    possibilities.add(new LogicalFilterExpression(Query.and(fields),
                            new RecordQueryScanPlan(matchedKeyComparisons.get().toScanComparisons(), false)));
                    break;
                }
            }
        }

        for (Index index : call.getContext().getIndexes()) {
            keyComparisons = new KeyExpressionComparisons(index.getRootExpression());
            for (FieldWithComparison field : fields) {
                matchedKeyComparisons = keyComparisons.matchWith(field);
                if (matchedKeyComparisons.isPresent()) {
                    possibilities.add(new LogicalFilterExpression(Query.and(fields),
                            new LogicalIndexScanExpression(index.getName(), IndexScanType.BY_VALUE,
                                    matchedKeyComparisons.get(), false)));
                    break;
                }
            }
        }

        if (possibilities.isEmpty()) {
            return ChangesMade.NO_CHANGE;
        }

        call.yield(SingleExpressionRef.of(new RelationalGroupRefHolder(possibilities)));
        return ChangesMade.MADE_CHANGES;
    }
}
