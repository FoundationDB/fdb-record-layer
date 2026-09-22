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
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
import java.util.Optional;
import java.util.Set;

/**
 * A table function expression that “explodes” a repeated field into a stream of its values.
 *
 * <p>In the {@code WITH ORDINALITY} variant, it also generates ordinals of the field values. In this case it
 * produces a struct with two anonymous fields—the element and the ordinal—instead of the bare element. The ordinals
 * are 1-based per the SQL standard, unless the expression is created {@linkplain #isZeroBasedOrdinality() 0-based}.
 *
 * <p>An explode that {@linkplain #flowsRecordConstructorValue() flows a record constructor} produces a struct in the
 * plain variant as well, with the element as its only field.
 */
@API(API.Status.EXPERIMENTAL)
public class ExplodeExpression extends AbstractRelationalExpressionWithoutChildren implements InternalPlannerGraphRewritable {
    @Nonnull
    private final Value collectionValue;

    /**
     * Whether ordinals should be produced alongside the array elements.
     */
    private final boolean withOrdinality;

    /**
     * Whether the ordinals produced are 0-based rather than 1-based.
     */
    private final boolean zeroBasedOrdinality;

    /**
     * The element type of the collection value.
     */
    @Nonnull
    private final Type elementType;

    /**
     * The type of the explode result.
     */
    @Nonnull
    private final Type explodeResultType;

    /**
     * Whether the value this expression flows is a record constructor of the element—and the ordinal, for the
     * {@code WITH ORDINALITY} variant—rather than one opaque value.
     */
    private final boolean flowsRecordConstructorValue;

    /**
     * The result value of the explode.
     */
    @Nonnull
    private final Value resultValue;

    public ExplodeExpression(@Nonnull final Value collectionValue, final boolean withOrdinality,
                             final boolean zeroBasedOrdinality, final boolean flowsRecordConstructorValue) {
        Verify.verify(withOrdinality || !zeroBasedOrdinality, "cannot base ordinals that are not produced");
        this.collectionValue = collectionValue;
        this.withOrdinality = withOrdinality;
        this.zeroBasedOrdinality = zeroBasedOrdinality;
        Verify.verify(collectionValue.getResultType().isArray());
        this.elementType = Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType());
        this.flowsRecordConstructorValue = flowsRecordConstructorValue;
        this.explodeResultType = explodeResultType(elementType, withOrdinality, flowsRecordConstructorValue);
        this.resultValue = explodeResultValue(elementType, withOrdinality, flowsRecordConstructorValue);
        Verify.verify(explodeResultType.equals(resultValue.getResultType()));
    }

    public ExplodeExpression(@Nonnull final Value collectionValue, final boolean withOrdinality,
                             final boolean zeroBasedOrdinality) {
        // a `WITH ORDINALITY` explode flows the element and the ordinal as a record constructor; the plain variant
        // has only the element
        this(collectionValue, withOrdinality, zeroBasedOrdinality, withOrdinality);
    }

    public ExplodeExpression(@Nonnull final Value collectionValue, final boolean withOrdinality) {
        this(collectionValue, withOrdinality, false);
    }

    public ExplodeExpression(@Nonnull final Value collectionValue) {
        this(collectionValue, false, false);
    }

    /**
     * Returns the element type of the collection value.
     */
    @Nonnull
    public Type getElementType() {
        return elementType;
    }

    /**
     * Returns the type of the explode result: the element type itself, or an anonymous-field struct holding the
     * element and, for the {@code WITH ORDINALITY} variant, the ordinal. The {@code WITH ORDINALITY} variant needs the
     * struct to carry the ordinal; the plain variant takes it only when the explode
     * {@linkplain #flowsRecordConstructorValue() flows a record constructor}.
     *
     * @param elementType the element type of the collection being exploded
     * @param withOrdinality whether ordinals are produced alongside the elements
     * @param flowsRecordConstructorValue whether the explode flows a record constructor
     * @return the result type of such an explode
     */
    @Nonnull
    public static Type explodeResultType(@Nonnull final Type elementType, final boolean withOrdinality,
                                         final boolean flowsRecordConstructorValue) {
        if (!withOrdinality && !flowsRecordConstructorValue) {
            return elementType;
        }
        final var fields = ImmutableList.<Type.Record.Field>builder();
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
    public Type getExplodeResultType() {
        return explodeResultType;
    }

    /**
     * Returns the value an explode of {@code elementType} flows, of the
     * {@linkplain #explodeResultType(Type, boolean, boolean) explode result type} either way: one opaque
     * {@link QueriedValue}, or a {@link RecordConstructorValue} whose columns are the element and, for the
     * {@code WITH ORDINALITY} variant, the ordinal.
     *
     * @param elementType the element type of the collection being exploded
     * @param withOrdinality whether ordinals are produced alongside the elements
     * @param flowsRecordConstructorValue whether the element and the ordinal are flowed as a record constructor
     * @return the value flowed by such an explode
     */
    @Nonnull
    public static Value explodeResultValue(@Nonnull final Type elementType, final boolean withOrdinality,
                                           final boolean flowsRecordConstructorValue) {
        if (!flowsRecordConstructorValue) {
            return new QueriedValue(explodeResultType(elementType, withOrdinality, false));
        }
        // Note: the element must stay the first column. `MaxMatchMap` returns the first reachable candidate value
        // that compares equal, and a `QueriedValue` has no identity beyond its class and result type, so an element
        // whose type is also a non-nullable `INT` -- the ordinal's type -- would just as happily match the ordinal
        // if the ordinal came first.
        //
        // The record is built nullable because `explodeResultType` declares it that way, and the callers verify
        // that the two agree: that declared type is what the plan looks its protobuf descriptor up by at run time.
        final var columns = ImmutableList.<Column<? extends Value>>builder();
        columns.add(Column.unnamedOf(new QueriedValue(elementType)));
        if (withOrdinality) {
            columns.add(Column.unnamedOf(new QueriedValue(Type.primitiveType(Type.TypeCode.INT, false))));
        }
        return RecordConstructorValue.ofColumns(columns.build(), true);
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

    public boolean isZeroBasedOrdinality() {
        return zeroBasedOrdinality;
    }

    /**
     * Returns whether the value this expression flows is a record constructor of the element—and the ordinal, for the
     * {@code WITH ORDINALITY} variant—rather than one opaque value. The plan implementing the expression takes this
     * over.
     */
    public boolean flowsRecordConstructorValue() {
        return flowsRecordConstructorValue;
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
                    isZeroBasedOrdinality() == other.isZeroBasedOrdinality() &&
                    semanticEqualsForResults(otherExpression, equivalencesMap);
        }
        return false;
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        // Note: This is written in a way that preserves pre-existing hashes for `withOrdinality=false` and for
        // (`withOrdinality=true` and `zeroBasedOrdinality=false`)
        if (!withOrdinality) {
            return Objects.hash(collectionValue);
        }
        return zeroBasedOrdinality
               ? Objects.hash(collectionValue, true, true)
               : Objects.hash(collectionValue, true);
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
            return new ExplodeExpression(translatedCollectionValue, withOrdinality, zeroBasedOrdinality,
                    flowsRecordConstructorValue);
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

        return exactlySubsumedBy(candidateExpression, bindingAliasMap, partialMatchMap, TranslationMap.empty());
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

    public static ExplodeExpression explodeField(@Nonnull final Quantifier.ForEach baseQuantifier,
                                                 @Nonnull final List<String> fieldNames) {
        return new ExplodeExpression(FieldValue.ofFieldNames(baseQuantifier.getFlowedObjectValue(), fieldNames));
    }
}
