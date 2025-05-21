/*
 * CellMergeRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierWithoutDefaultOnEmptyOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * Rule for merging related select boxes into a single, larger select box.
 */
public class CellMergeRule extends CascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<Reference> ref = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> qun = forEachQuantifierWithoutDefaultOnEmptyOverRef(ref);
    @Nonnull
    private static final BindingMatcher<SelectExpression> root = selectExpression(qun);

    public CellMergeRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final SelectExpression select = call.get(root);
        final Quantifier.ForEach child = call.get(qun);
        final Reference childRef = call.get(ref);

        // todo: worry about correlation dependencies
        for (RelationalExpression childExpr : childRef.getExploratoryExpressions()) {
            MergeChildQuantifiersVisitor mergeVisitor = new MergeChildQuantifiersVisitor(child.getAlias());
            if (mergeVisitor.visit(childExpr)) {
                final ImmutableList.Builder<Quantifier> children = ImmutableList.builder();

                // Replace the matched quantifier with it child quantifiers
                for (Quantifier selectQun : select.getQuantifiers()) {
                    if (selectQun.equals(child)) {
                        children.addAll(Objects.requireNonNull(mergeVisitor.childQuantifiers));
                    } else {
                        children.add(selectQun);
                    }
                }

                // Combine the predicates. These come from two sources: one are the
                // child predicates, which can be pulled up directly.
                // The rest are the upper predicates, which need to be translated so that any
                // references to the removed value now applies to the new value.
                final ImmutableList.Builder<QueryPredicate> predicates = ImmutableList.builder();
                predicates.addAll(Objects.requireNonNull(mergeVisitor.predicates));

                TranslationMap translationMap = Objects.requireNonNull(mergeVisitor.translationMap);
                select.getPredicates().forEach(predicate -> predicates.add(predicate.translateCorrelations(translationMap, true)));

                // Translate the result value in the same way
                Value newResultValue = select.getResultValue().translateCorrelations(translationMap, true);

                //
                // Yield a new select merging the existing select with the child
                //
                call.yieldExploratoryExpression(new SelectExpression(newResultValue, children.build(), predicates.build()));
            }
        }
    }

    private static final class MergeChildQuantifiersVisitor implements RelationalExpressionVisitorWithDefaults<Boolean> {
        @Nonnull
        private final CorrelationIdentifier parentId;
        @Nullable
        private TranslationMap translationMap;
        @Nullable
        private List<? extends Quantifier> childQuantifiers;
        @Nullable
        private List<? extends QueryPredicate> predicates;

        public MergeChildQuantifiersVisitor(@Nonnull CorrelationIdentifier parentId) {
            this.parentId = parentId;
        }

        @Nonnull
        @Override
        public Boolean visitLogicalFilterExpression(@Nonnull final LogicalFilterExpression filterExpression) {
            //
            // Logical filters can be absorbed by parent selects. Any references to the filter
            // expression need to be rewritten in terms of the original inner alias
            //
            translationMap = TranslationMap.rebaseWithAliasMap(
                    AliasMap.ofAliases(parentId, filterExpression.getInner().getAlias()));
            childQuantifiers = filterExpression.getQuantifiers();
            Verify.verify(childQuantifiers.size() == 1, "logical filter expressions should always have exactly 1 child quantifier");
            predicates = filterExpression.getPredicates();
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitSelectExpression(@Nonnull final SelectExpression select) {
            //
            // Select expressions can be merged with higher selects.
            // References should be rewritten using this expression's base
            // value in place of the original reference
            //
            translationMap = TranslationMap.builder()
                    .when(parentId)
                    .then((alias, leaf) -> select.getResultValue())
                    .build();
            childQuantifiers = select.getQuantifiers();
            predicates = select.getPredicates();
            return true;
        }

        @Nonnull
        @Override
        public Boolean visitDefault(@Nonnull final RelationalExpression element) {
            // By default, we cannot merge the expression into a higher select
            return false;
        }
    }
}
