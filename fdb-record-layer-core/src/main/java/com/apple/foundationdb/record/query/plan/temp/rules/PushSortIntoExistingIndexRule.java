/*
 * PushSortIntoExistingIndexRule.java
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
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
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
 * A rule that pushes a sort into a compatibly-ordered index scan, including one with existing comparisons
 * (from filters or other sort orders) that might affect whether the index is compatibly ordered.
 *
 * <pre>
 * {@code
 *       +-----------------------------+                 +---------------------------+
 *       |                             |                 |                           |
 *       |  LogicalSortExpression      |                 |  LogicalSortExpression    |
 *       |            prefix, suffix   |                 |                 suffix    |
 *       |                             |                 |                           |
 *       +-------------+---------------+                 +-------------+-------------+
 *                     |                     +------>                  |
 *                     | qun                                           | newQun
 *                     |                                               |
 *     +---------------+------------------+            +---------------+------------------+
 *     |                                  |            |                                  |
 *     |  IndexEntrySourceScanExpression  |            |  IndexEntrySourceScanExpression  |
 *     |                           orders |            |                     order|prefix |
 *     |                                  |            |                                  |
 *     +----------------------------------+            +----------------------------------+
 * }
 * </pre>
 *
 * or if there are no other predicates
 *
 * <pre>
 * {@code
 *       +-----------------------------+               +----------------------------------+
 *       |                             |               |                                  |
 *       |    LogicalSortExpression    |     +------>  |  IndexEntrySourceScanExpression  |
 *       |                     prefix  |               |                           orders |
 *       |                             |               |                                  |
 *       +-------------+---------------+               +----------------------------------+
 *                     |
 *                     | qun
 *                     |
 *     +---------------+------------------+
 *     |                                  |
 *     |  IndexEntrySourceScanExpression  |
 *     |                          orders  |
 *     |                                  |
 *     +----------------------------------+
 * }
 * </pre>
 */
@API(API.Status.EXPERIMENTAL)
public class PushSortIntoExistingIndexRule extends PlannerRule<LogicalSortExpression> {
    private static final ExpressionMatcher<IndexEntrySourceScanExpression> indexScanMatcher = TypeMatcher.of(IndexEntrySourceScanExpression.class);
    private static final QuantifierMatcher<Quantifier.ForEach> qunMatcher = QuantifierMatcher.forEach(indexScanMatcher);
    private static final ExpressionMatcher<LogicalSortExpression> root =
            TypeMatcher.of(LogicalSortExpression.class, qunMatcher);

    public PushSortIntoExistingIndexRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final IndexEntrySourceScanExpression scan = call.get(indexScanMatcher);
        final Quantifier.ForEach qun = call.get(qunMatcher);
        final LogicalSortExpression sortExpression = call.get(root);

        final Optional<ViewExpressionComparisons> matchedViewExpression = scan.getComparisons()
                .matchWithSort(sortExpression.getSortPrefix());
        if (matchedViewExpression.isPresent() && scan.isReverse() == sortExpression.isReverse()) {
            final ExpressionRef<? extends RelationalExpression> indexScanRef =
                    call.ref(new IndexEntrySourceScanExpression(
                            scan.getIndexEntrySource(),
                            scan.getScanType(),
                            matchedViewExpression.get(),
                            scan.isReverse()));
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

                final LogicalSortExpression newSortExpression =
                        new LogicalSortExpression(
                                rebasedPrefix,
                                rebasedSuffix,
                                sortExpression.isReverse(),
                                newQun);
                call.yield(call.ref(newSortExpression));
            }
        }
    }
}
