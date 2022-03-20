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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.QueriedValue;
import com.apple.foundationdb.record.query.predicates.Value;
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
        if (collectionValue.getResultType().getTypeCode() == Type.TypeCode.ARRAY) {
            final Type innerType = Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType());
            return new QueriedValue(innerType);
        } else {
            // TODO currently needed for index candidate compilation
            return new QueriedValue(Type.primitiveType(Type.TypeCode.UNKNOWN));
        }
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
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        return getClass() == otherExpression.getClass() &&
               semanticEqualsForResults(otherExpression, equivalencesMap);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return 17;
    }

    @Nonnull
    @Override
    public ExplodeExpression rebase(@Nonnull final AliasMap translationMap) {
        final Value rebasedResultValue = collectionValue.rebase(translationMap);
        if (rebasedResultValue == this.collectionValue) {
            return this;
        } else {
            return new ExplodeExpression(rebasedResultValue);
        }
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
