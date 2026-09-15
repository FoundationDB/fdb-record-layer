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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A table function expression that “explodes” a repeated field into a stream of its values.
 *
 * <p>In the {@code WITH ORDINALITY} variant, it also generates ordinals of the field values. In this case it
 * produces a struct with two anonymous fields—the element and the ordinal—instead of the bare element. The ordinals
 * are <em>0-based</em>.
 */
@API(API.Status.EXPERIMENTAL)
public class ExplodeExpression extends AbstractRelationalExpressionWithoutChildren implements InternalPlannerGraphRewritable {
    /**
     * The ordinal of the array element in the result struct.
     */
    public static final int ELEMENT_ORDINAL = 0;

    /**
     * The ordinal of the ordinality in the result struct, for the {@code WITH ORDINALITY} variant.
     */
    public static final int ORDINALITY_ORDINAL = 1;

    @Nonnull
    private final Value collectionValue;

    /**
     * Whether ordinals should be produced alongside the array elements.
     */
    private final boolean withOrdinality;

    /**
     * The type of the explode result.
     */
    @Nonnull
    private final Type.Record explodeResultType;

    /**
     * The result value of the explode.
     */
    @Nonnull
    private final Value resultValue;

    public ExplodeExpression(@Nonnull final Value collectionValue, final boolean withOrdinality) {
        this.collectionValue = collectionValue;
        this.withOrdinality = withOrdinality;
        Verify.verify(collectionValue.getResultType().isArray());
        final Type elementType = Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType());
        this.explodeResultType = explodeResultType(elementType, withOrdinality);
        this.resultValue = explodeResultValue(elementType, withOrdinality);
        Verify.verify(explodeResultType.equals(resultValue.getResultType()));
    }

    public ExplodeExpression(@Nonnull final Value collectionValue) {
        this(collectionValue, false);
    }

    /**
     * Returns the type of the explode result: an anonymous-field struct holding the element, and the ordinal as well
     * for the {@code WITH ORDINALITY} variant.
     */
    @Nonnull
    public static Type.Record explodeResultType(@Nonnull final Type elementType, boolean withOrdinality) {
        final ImmutableList.Builder<Type.Record.Field> fields = ImmutableList.builder();
        fields.add(Type.Record.Field.of(elementType, Optional.empty()));
        if (withOrdinality) {
            fields.add(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.empty()));
        }
        return Type.Record.fromFields(fields.build());
    }

    /**
     * Returns the type of the explode result.
     */
    @Nonnull
    public Type.Record getExplodeResultType() {
        return explodeResultType;
    }

    @Nonnull
    public static Value explodeResultValue(@Nonnull final Type elementType, final boolean withOrdinality) {
        final List<Column<? extends Value>> columns = new ArrayList<>();
        columns.add(Column.unnamedOf(new QueriedValue(elementType)));
        if (withOrdinality) {
            columns.add(Column.unnamedOf(new QueriedValue(Type.primitiveType(Type.TypeCode.INT, false))));
        }
        return RecordConstructorValue.ofColumns(columns, true);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    public Value getCollectionValue() {
        return collectionValue;
    }

    public boolean isWithOrdinality() {
        return withOrdinality;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return collectionValue.getCorrelatedTo();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (otherExpression instanceof final ExplodeExpression other) {
            return collectionValue.semanticEquals(other.getCollectionValue(), equivalencesMap) &&
                    isWithOrdinality() == other.isWithOrdinality() &&
                    semanticEqualsForResults(otherExpression, equivalencesMap);
        }
        return false;
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        // Note: This is written in a way that preserves pre-existing hashes for `withOrdinality=false`.
        return withOrdinality
               ? Objects.hash(collectionValue, true)
               : Objects.hash(collectionValue);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ExplodeExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                   final boolean shouldSimplifyValues,
                                                   @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        final Value translatedCollectionValue =
                collectionValue.translateCorrelations(translationMap, shouldSimplifyValues);
        // this is ok since there are no new quantifiers
        if (translatedCollectionValue != collectionValue) {
            return new ExplodeExpression(translatedCollectionValue, withOrdinality);
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

        if (!withOrdinality
                && candidateExpression instanceof final ExplodeExpression candidateExplodeExpression
                && candidateExplodeExpression.isWithOrdinality()) {
            return subsumedByWithOrdinality(candidateExplodeExpression, bindingAliasMap, partialMatchMap);
        }

        return exactlySubsumedBy(candidateExpression, bindingAliasMap, partialMatchMap, TranslationMap.empty());
    }

    @Nonnull
    private Iterable<MatchInfo> subsumedByWithOrdinality(@Nonnull final ExplodeExpression candidateExpression,
                                                         @Nonnull final AliasMap bindingAliasMap,
                                                         @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        if (!collectionValue.semanticEquals(candidateExpression.getCollectionValue(), bindingAliasMap)) {
            return ImmutableList.of();
        }

        final var maxMatchMap =
                MaxMatchMap.compute(getResultValue(), candidateExpression.getResultValue(),
                        Quantifiers.aliases(candidateExpression.getQuantifiers()));

        return MatchInfo.RegularMatchInfo.tryFromMatchMap(bindingAliasMap, partialMatchMap, maxMatchMap)
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Nonnull
    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nullable final PullUp pullUp,
                                   @Nonnull final CorrelationIdentifier candidateAlias) {
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
        return withOrdinality
               ? collectionValue + " WITH ORDINALITY"
               : collectionValue.toString();
    }

    @Nonnull
    public Type getElementType() {
        return explodeResultType.getField(ELEMENT_ORDINAL).getFieldType();
    }

    @Nonnull
    public static Value elementValueOf(@Nonnull final Quantifier explodeQuantifier) {
        return FieldValue.ofOrdinalNumber(explodeQuantifier.getFlowedObjectValue(), ELEMENT_ORDINAL);
    }

    @Nonnull
    public static ExplodeExpression explodeField(@Nonnull final Quantifier.ForEach baseQuantifier,
                                                 @Nonnull final List<String> fieldNames) {
        return explodeField(baseQuantifier.getFlowedObjectValue(), fieldNames);
    }

    @Nonnull
    public static ExplodeExpression explodeField(@Nonnull final Value baseValue,
                                                 @Nonnull final List<String> fieldNames) {
        return new ExplodeExpression(FieldValue.ofFieldNames(baseValue, fieldNames));
    }
}
