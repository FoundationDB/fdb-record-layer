/*
 * FindPossibleIndexForAndPredicateRule.java
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
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A rule that finds all indexes that could implement one of the {@link ComponentWithComparison} conjuncts of an AND
 * filter, leaving all the other filters (of any type, including other fields) as a residual filter.
 *
 * <pre>
 * {@code
 *                                                     index 1:                                        index n:
 *      +----------------------------+                     +------------------------------+               +------------------------------+
 *      |                            |                     |                              |               |                              |
 *      |  LogicalFilterExpression   |                     |  LogicalFilterExpression     |               |  LogicalFilterExpression     |
 *      |          element <>= val   |                     |              residual preds  |               |              residual preds  |
 *      |                            |                     |                              |               |                              |
 *      +-------------+--------------+                     +---------------+--------------+               +---------------+--------------+
 *                    |                    +-------->                      |                    ...                       |
 *                    |  qun                                               |                                              |
 *                    |                                                    |                                              |
 *     +--------------+----------------+               +-------------------+-----------------+        +-------------------+-----------------+
 *     |                               |               |                                     |        |                                     |
 *     |  FullUnorderedScanExpression  |               |  IndexEntrySourceScanExpression     |        |  IndexEntrySourceScanExpression     |
 *     |                               |               |                        scan ranges  |        |                        scan ranges  |
 *     +-------------------------------+               |                                     |        |                                     |
 * }                                                   +-------------------------------------+        +-------------------------------------+
 * </pre>
 *
 */
@API(API.Status.EXPERIMENTAL)
public class FindPossibleIndexForAndPredicateRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<ElementPredicate> elementPredMatcher = TypeMatcher.of(ElementPredicate.class);
    private static final ExpressionMatcher<QueryPredicate> residualPredMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<AndPredicate> andFilterMatcher =
            TypeMatcher.of(AndPredicate.class, AnyChildWithRestMatcher.anyMatchingWithRest(elementPredMatcher, residualPredMatcher));
    private static final ExpressionMatcher<Quantifier.ForEach> qunMatcher = QuantifierMatcher.forEach(TypeMatcher.of(FullUnorderedScanExpression.class));
    private static final ExpressionMatcher<LogicalFilterExpression> root =
            TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class,
                    andFilterMatcher,
                    qunMatcher);

    public FindPossibleIndexForAndPredicateRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalFilterExpression filterExpression = call.get(root);
        final ElementPredicate field = call.get(elementPredMatcher);
        final Quantifier.ForEach qun = call.get(qunMatcher);

        for (IndexEntrySource indexEntrySource : call.getContext().getIndexEntrySources()) {
            final ViewExpressionComparisons comparisons = indexEntrySource.getEmptyComparisons();
            final Optional<ViewExpressionComparisons> matchedKeyComparisons = comparisons.matchWith(field);

            // TODO revisit later -- this is too restrictive
            if (!field.getCorrelatedTo().isEmpty()) {
                continue;
            }

            if (matchedKeyComparisons.isPresent()) {
                final List<QueryPredicate> residualPreds = call.getBindings().getAll(residualPredMatcher);
                final Quantifier.ForEach newQun =
                        Quantifier.forEach(
                                call.ref(new IndexEntrySourceScanExpression(indexEntrySource, IndexScanType.BY_VALUE, matchedKeyComparisons.get(), false)));
                final AliasMap translationMap = Quantifiers.translate(qun, newQun);
                final QueryPredicate residualFilter;
                if (residualPreds.size() == 1) {
                    residualFilter = residualPreds.get(0).rebase(translationMap);
                } else {
                    residualFilter = new AndPredicate(
                            residualPreds.stream()
                                    .map(pred -> pred.rebase(translationMap))
                                    .collect(Collectors.toList()));
                }
                call.yield(call.ref(new LogicalFilterExpression(
                        filterExpression.getBaseSource(),
                        residualFilter,
                        newQun)));
            }
        }
    }
}
