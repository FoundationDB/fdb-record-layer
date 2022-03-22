/*
 * DataAccessRule.java
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
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.MatchPartition;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.PrimaryScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.NotMatcher.not;
import static com.apple.foundationdb.record.query.plan.temp.matchers.PartialMatchMatchers.completeMatch;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.anyExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.ofType;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create one or more
 * expressions for data access. While this rule delegates specifics to the {@link MatchCandidate}s, the following are
 * possible outcomes of the application of this transformation rule. Based on the match info, we may create for a single match:
 *
 * <ul>
 *     <li>a {@link PrimaryScanExpression} for a single {@link PrimaryScanMatchCandidate},</li>
 *     <li>an {@link IndexScanExpression} for a single {@link ValueIndexScanMatchCandidate}</li>
 *     <li>an intersection ({@link LogicalIntersectionExpression}) of data accesses </li>
 * </ul>
 *
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class DataAccessRule extends AbstractDataAccessRule<RelationalExpression> {
    private static final BindingMatcher<PartialMatch> completeMatchMatcher = completeMatch();
    private static final BindingMatcher<RelationalExpression> expressionMatcher = anyExpression().where(not(ofType(SelectExpression.class)));

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, some(completeMatchMatcher));
    
    public DataAccessRule() {
        super(rootMatcher, completeMatchMatcher, expressionMatcher);
    }

    @Nonnull
    @Override
    protected ExpressionRef<? extends RelationalExpression> inject(@Nonnull RelationalExpression expression,
                                                                   @Nonnull List<? extends PartialMatch> completeMatches,
                                                                   @Nonnull final ExpressionRef<? extends RelationalExpression> compensatedScanGraph) {
        Verify.verify(computeIntersectedUnmatchedForEachQuantifiers(expression, completeMatches).isEmpty());
        return compensatedScanGraph;
    }
}
