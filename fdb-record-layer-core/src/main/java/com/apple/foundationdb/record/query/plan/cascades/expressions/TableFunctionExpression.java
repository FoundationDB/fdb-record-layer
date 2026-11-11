/*
 * TableFunctionExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo.RegularMatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ExplodeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A table function expression that delegates the actual execution to an underlying {@link StreamingValue} which
 * effectively returns a stream of results.
 */
@API(API.Status.EXPERIMENTAL)
public class TableFunctionExpression extends AbstractRelationalExpressionWithoutChildren implements InternalPlannerGraphRewritable {
    @Nonnull
    private final StreamingValue value;

    public TableFunctionExpression(@Nonnull final StreamingValue value) {
        this.value = value;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(value.getResultType());
    }

    @Nonnull
    @Override
    protected Set<Type> computeDynamicTypes() {
        return value.getDynamicTypes();
    }

    @Nonnull
    public StreamingValue getValue() {
        return value;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return value.getCorrelatedTo();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof TableFunctionExpression)) {
            return false;
        }

        final var otherTableFunctionExpression = (TableFunctionExpression)otherExpression;

        return value.semanticEquals(otherTableFunctionExpression.getValue(), equivalencesMap);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(value);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public TableFunctionExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                   final boolean shouldSimplifyValues,
                                                   @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }
        final Value translatedCollectionValue = value.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedCollectionValue != value) {
            return new TableFunctionExpression((StreamingValue)translatedCollectionValue);
        }
        return this;
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap bindingAliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {
        if (!isCompatiblyAndCompletelyBound(bindingAliasMap, candidateExpression.getQuantifiers())) {
            return ImmutableList.of();
        }

        //
        // An explode that does not preserve its order is subsumed by an explode of the same collection that does,
        // such as the one an index on an unnested record type is expanded with. Both flow the same
        // (element, ordinal) struct, one per element of the collection, and the query does not observe its ordinals,
        // so the candidate produces everything the query may produce. The explodes do not compare equal, so
        // exactlySubsumedBy() would reject the match. A sorted query explode is left to exactlySubsumedBy(), as the
        // candidate does not produce its elements in the order the query asks for.
        //
        if (value instanceof ExplodeValue queryExplodeValue
                && !queryExplodeValue.preservesOrder()
                && !queryExplodeValue.isSorted()
                && candidateExpression instanceof TableFunctionExpression candidateTableFunctionExpression
                && candidateTableFunctionExpression.getValue() instanceof ExplodeValue candidateExplodeValue
                && candidateExplodeValue.preservesOrder()) {
            if (!queryExplodeValue.getCollectionValue()
                    .semanticEquals(candidateExplodeValue.getCollectionValue(), bindingAliasMap)
                    || !queryExplodeValue.getElementType().equals(candidateExplodeValue.getElementType())) {
                return ImmutableList.of();
            }
            // CollectionValue and ElementType match
            final var maxMatchMap =
                    MaxMatchMap.identityOf(getResultValue(), candidateExpression.getResultValue(),
                            Quantifiers.aliases(candidateExpression.getQuantifiers()),
                            ValueEquivalence.fromAliasMap(bindingAliasMap));
            return RegularMatchInfo.tryFromMatchMap(bindingAliasMap, partialMatchMap, maxMatchMap)
                    .map(ImmutableList::of)
                    .orElse(ImmutableList.of());
        }

        return exactlySubsumedBy(candidateExpression, bindingAliasMap, partialMatchMap, TranslationMap.empty());
    }

    @Nonnull
    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nullable final PullUp pullUp,
                                   @Nonnull final CorrelationIdentifier nestingAlias) {
        // subsumedBy() is based on equality and this expression is always a leaf, thus we return empty here as
        // if there is a match, it's exact
        return Compensation.noCompensation();
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "TFunc",
                        ImmutableList.of(toString()),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
