/*
 * MatchLeafRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRefTraversal;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.IterableHelpers;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.BoundMatch;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * Expression-based transformation rule that matches any leaf expression. The rule creates matches of type
 * {@link PartialMatch} for any match between this expression and a leaf expression in a {@link MatchCandidate}.
 * It seeds the memoization structure for partial matches that is kept as part of {@link ExpressionRef}. It prepares
 * further rules such as {@link MatchIntermediateRule} and {@link AdjustMatchRule}.
 */
@API(API.Status.EXPERIMENTAL)
public class MatchLeafRule extends CascadesRule<RelationalExpression> {
    // match any relational expression that is a leaf, that is, any expression that does not have any children
    private static final BindingMatcher<RelationalExpression> root =
            RelationalExpressionMatchers.ofTypeOwning(RelationalExpression.class, CollectionMatcher.empty());

    public MatchLeafRule() {
        super(root);
    }

    /**
     * Note: Transformation rules for expressions are partitioned by the class of the root matcher. This does not work
     * here as this rule is non-specific which means that it matches sub classes of {@link RelationalExpression} and not
     * just {@link RelationalExpression} itself. In order for this rule to fall into the set of rules that is always
     * utilized we return {@code Optional.empty()} here.
     * @return {@code Optional.empty()}
     */
    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlanContext context = call.getContext();
        final RelationalExpression expression = call.get(root);
        // iterate through all candidates known to the context
        for (final MatchCandidate matchCandidate : context.getMatchCandidates()) {
            final ExpressionRefTraversal traversal = matchCandidate.getTraversal();
            final Set<ExpressionRef<? extends RelationalExpression>> leafRefs = traversal.getLeafReferences();
            // iterate through all leaf references in all
            for (final ExpressionRef<? extends RelationalExpression> leafRef : leafRefs) {
                for (final RelationalExpression leafMember : leafRef.getMembers()) {
                    // A leaf reference strictly is a reference that contains at least one member expression that does
                    // not have any quantifiers it ranges over. We need to make sure that we actually filter out all
                    // member expressions that do have quantifiers they range over as we are interested in only the leaf
                    // expressions.
                    if (leafMember.getQuantifiers().isEmpty()) {
                        final Iterable<BoundMatch<MatchInfo>> boundMatchInfos = matchWithCandidate(expression, leafMember, call.getEvaluationContext());
                        // yield any match to the planner
                        boundMatchInfos.forEach(boundMatchInfo ->
                                call.yieldPartialMatch(boundMatchInfo.getAliasMap(),
                                        matchCandidate,
                                        expression,
                                        leafRef,
                                        boundMatchInfo.getMatchResult()));
                    }
                }
            }
        }
    }

    /**
     * Method to match an expression with a candidate expression.
     * @param expression an expression
     * @param candidateExpression a candidate expression
     * @return an {@link Iterable} of bound {@link MatchInfo}s where each match info represents a math under the bound
     *         mappings (between query expression and candidate expression).
     */
    private Iterable<BoundMatch<MatchInfo>> matchWithCandidate(@Nonnull RelationalExpression expression,
                                                               @Nonnull RelationalExpression candidateExpression,
                                                               @Nonnull final EvaluationContext context) {
        // Enumerate all possibilities for aliases that this expression and the candidate expression are correlated to.
        final Iterable<AliasMap> boundCorrelatedIterable =
                expression.enumerateUnboundCorrelatedTo(AliasMap.emptyMap(), candidateExpression);

        // Flatmap each bound correlation mapping through the subsumption method. Note, that subsumedBy is allowed to
        // create 0 to n actual matches between this expression and the candidate expression although in reality it will
        // be mostly only zero or one.
        return IterableHelpers.flatMap(boundCorrelatedIterable,
                boundAliasMap ->
                        IterableHelpers.map(expression.subsumedBy(candidateExpression, boundAliasMap, IdentityBiMap.create(), context),
                                matchInfo ->
                                        BoundMatch.withAliasMapAndMatchResult(boundAliasMap, matchInfo)));
    }
}
