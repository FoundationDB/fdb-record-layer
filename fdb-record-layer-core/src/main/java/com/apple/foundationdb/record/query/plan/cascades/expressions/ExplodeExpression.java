/*
 * ExplodeExpression.java
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A table function expression that "explodes" a repeated field into a stream of its values.
 */
@API(API.Status.EXPERIMENTAL)
public class ExplodeExpression implements RelationalExpression, InternalPlannerGraphRewritable {
    @Nonnull
    private final Value collectionValue;

    public ExplodeExpression(@Nonnull final Value collectionValue) {
        this.collectionValue = collectionValue;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        Verify.verify(collectionValue.getResultType().getTypeCode() == Type.TypeCode.ARRAY);

        return new QueriedValue(Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType()));
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        return collectionValue.getDynamicTypes();
    }

    @Nonnull
    public Value getCollectionValue() {
        return collectionValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return collectionValue.getCorrelatedTo();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof ExplodeExpression)) {
            return false;
        }

        final var otherExplodeExpression = (ExplodeExpression)otherExpression;

        return collectionValue.semanticEquals(otherExplodeExpression.getCollectionValue(), equivalencesMap) &&
               semanticEqualsForResults(otherExpression, equivalencesMap);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(collectionValue);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ExplodeExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final Value translatedCollectionValue = collectionValue.translateCorrelations(translationMap);
        if (translatedCollectionValue != collectionValue) {
            return new ExplodeExpression(translatedCollectionValue);
        }
        return this;
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression, @Nonnull final AliasMap aliasMap, @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return exactlySubsumedBy(candidateExpression, aliasMap, partialMatchMap);
    }

    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        // subsumedBy() is based on equality and this expression is always a leaf, thus we return empty here as
        // if there is a match, it's exact
        return Compensation.noCompensation();
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "Explode",
                        ImmutableList.of(toString()),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public String toString() {
        return collectionValue.toString();
    }

    public static ExplodeExpression explodeField(@Nonnull final Quantifier.ForEach baseQuantifier,
                                                 @Nonnull final List<String> fieldNames) {
        return new ExplodeExpression(new FieldValue(baseQuantifier.getFlowedObjectValue(), fieldNames));
    }
}
