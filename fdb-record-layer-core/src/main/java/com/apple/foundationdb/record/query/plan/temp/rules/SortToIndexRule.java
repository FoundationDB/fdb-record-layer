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
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.IndexEntrySource;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpressionOld;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A rule for implementing a {@link LogicalSortExpressionOld} as a scan of any appropriately-ordered index.
 * The rule's logic mirrors {@link FilterWithElementWithComparisonRule}, but applied to sorts rather than filters.
 *
 * <pre>
 * {@code
 *       +--------------------------------+                 +------------------------------+
 *       |                                |                 |                              |
 *       |  LogicalSortExpressionOld      |                 |  LogicalSortExpressionOld    |
 *       |            prefix, suffix      |                 |                  suffix      |
 *       |                                |                 |                              |
 *       +-------------+------------------+                 +-------------+----------------+
 *                     |                     +------>                     |
 *                     | qun                                              | newQun
 *                     |                                                  |
 *     +---------------+------------------+               +---------------+------------------+
 *     |                                  |               |                                  |
 *     |  FullUnorderedScanExpression     |               |  IndexEntrySourceScanExpression  |
 *     |                                  |               |                     order|prefix |
 *     +----------------------------------+               |                                  |
 *                                                        +----------------------------------+
 * }
 * </pre>
 *
 * or if there is no suffix:
 *
 * <pre>
 * {@code
 *       +--------------------------------+               +----------------------------------+
 *       |                                |               |                                  |
 *       |    LogicalSortExpressionOld    |     +------>  |  IndexEntrySourceScanExpression  |
 *       |                     prefix     |               |                           orders |
 *       |                                |               |                                  |
 *       +-------------+------------------+               +----------------------------------+
 *                     |
 *                     | qun
 *                     |
 *     +---------------+------------------+
 *     |                                  |
 *     |  FullUnorderedScanExpression     |
 *     |                                  |
 *     +----------------------------------+
 * }
 * </pre>
 */
@API(API.Status.EXPERIMENTAL)
public class SortToIndexRule extends PlannerRule<LogicalSortExpressionOld> {
    private static final ExpressionMatcher<FullUnorderedScanExpression> innerMatcher = TypeMatcher.of(FullUnorderedScanExpression.class);
    private static final QuantifierMatcher<Quantifier.ForEach> qunMatcher = QuantifierMatcher.forEach(innerMatcher);
    private static final ExpressionMatcher<LogicalSortExpressionOld> root =
            TypeMatcher.of(LogicalSortExpressionOld.class, qunMatcher);

    public SortToIndexRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalSortExpressionOld sortExpression = call.get(root);
        final Quantifier.ForEach qun = call.get(qunMatcher);
        final boolean reverse = call.get(root).isReverse();

        for (IndexEntrySource indexEntrySource : call.getContext().getIndexEntrySources()) {
            final ViewExpressionComparisons comparisons = indexEntrySource.getEmptyComparisons();
            final Optional<ViewExpressionComparisons> matchedViewExpression = comparisons.matchWithSort(sortExpression.getSortPrefix());
            if (matchedViewExpression.isPresent()) {
                final ExpressionRef<? extends RelationalExpression> indexScanRef =
                        call.ref(new IndexEntrySourceScanExpression(
                                indexEntrySource,
                                IndexScanType.BY_VALUE,
                                matchedViewExpression.get(),
                                reverse));
                if (sortExpression.getSortSuffix().isEmpty()) {
                    call.yield(indexScanRef);
                } else {
                    final Quantifier.ForEach newQun = Quantifier.forEach(indexScanRef);
                    final AliasMap translationMap = Quantifiers.translate(qun, newQun);

                    final List<Element> rebasedPrefix =
                            sortExpression.getSortPrefix()
                                    .stream()
                                    .map(element -> element.rebase(translationMap))
                                    .collect(Collectors.toList());

                    final List<Element> rebasedSuffix =
                            sortExpression.getSortSuffix()
                                    .stream()
                                    .map(element -> element.rebase(translationMap))
                                    .collect(Collectors.toList());

                    call.yield(call.ref(new LogicalSortExpressionOld(
                            rebasedPrefix,
                            rebasedSuffix,
                            reverse,
                            newQun)));
                }
            }
        }
    }
}
