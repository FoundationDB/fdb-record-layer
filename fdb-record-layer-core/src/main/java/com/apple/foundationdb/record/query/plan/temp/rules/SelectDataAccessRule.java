/*
 * SelectDataAccessRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.MatchPartition;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.PartialMatchMatchers.completeMatch;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.ofType;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create one or more
 * expressions for data access specifically for a {@link SelectExpression}. A {@link SelectExpression} is behaves
 * different than essentially all other expressions in a way that we can break such an expression on the fly
 * and only replace the matched part of the original expression with the scan over the materialized view. That allows
 * us to relax restrictions (.e.g. to match all quantifiers the select expression owns) while matching select expressions.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class SelectDataAccessRule extends AbstractDataAccessRule<SelectExpression> {
    private static final BindingMatcher<PartialMatch> completeMatchMatcher = completeMatch();
    private static final BindingMatcher<SelectExpression> expressionMatcher = ofType(SelectExpression.class);

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, some(completeMatchMatcher));

    public SelectDataAccessRule() {
        super(rootMatcher, completeMatchMatcher, expressionMatcher);
    }

    @Nonnull
    @Override
    protected ExpressionRef<? extends RelationalExpression> inject(@Nonnull SelectExpression selectExpression,
                                                                   @Nonnull List<? extends PartialMatch> completeMatches,
                                                                   @Nonnull final ExpressionRef<? extends RelationalExpression> compensatedScanGraph) {
        final Set<Quantifier.ForEach> unmatchedQuantifiers =
                computeIntersectedUnmatchedForEachQuantifiers(selectExpression, completeMatches);
        if (unmatchedQuantifiers.isEmpty()) {
            return compensatedScanGraph;
        }
        
        //
        // Create a new SelectExpression that contains all the unmatched for each quantifiers as well as the
        // compensated scan graph.
        //
        final ImmutableList.Builder<Quantifier.ForEach> allQuantifiersBuilder = ImmutableList.builder();
        unmatchedQuantifiers.stream()
                .map(quantifier -> Quantifier.forEachBuilder().from(quantifier).build(quantifier.getRangesOver()))
                .forEach(allQuantifiersBuilder::add);
        final Quantifier.ForEach compensatedScanQuantifier = Quantifier.forEach(compensatedScanGraph);
        allQuantifiersBuilder.add(compensatedScanQuantifier);

        return GroupExpressionRef.of(new SelectExpression(compensatedScanQuantifier.getFlowedObjectValue(), allQuantifiersBuilder.build(), ImmutableList.of()));
    }
}
