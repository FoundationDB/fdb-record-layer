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
     * The result value of the explode.
     */
    @Nonnull
    private final Value resultValue;

    public ExplodeExpression(@Nonnull final Value collectionValue, final boolean withOrdinality,
                             final boolean zeroBasedOrdinality) {
        Verify.verify(withOrdinality || !zeroBasedOrdinality, "cannot base ordinals that are not produced");
        this.collectionValue = collectionValue;
        this.withOrdinality = withOrdinality;
        this.zeroBasedOrdinality = zeroBasedOrdinality;
        Verify.verify(collectionValue.getResultType().isArray());
        this.elementType = Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType());
        this.explodeResultType = explodeResultType(elementType, withOrdinality);
        this.resultValue = explodeResultValue(elementType, withOrdinality);
        Verify.verify(explodeResultType.equals(resultValue.getResultType()));
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
     * Returns the type of the explode result. For the {@code WITH ORDINALITY} variant, builds an anonymous-field
     * struct result type holding the element and the ordinal.
     */
    @Nonnull
    public static Type explodeResultType(@Nonnull final Type elementType, boolean withOrdinality) {
        if (withOrdinality) {
            return Type.Record.fromFields(ImmutableList.of(
                    Type.Record.Field.of(elementType, Optional.empty()),
                    Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.empty())));
        } else {
            return elementType;
        }
    }

    /**
     * Returns the type of the explode result.
     */
    @Nonnull
    public Type getExplodeResultType() {
        return explodeResultType;
    }

    /**
     * Returns the value an explode of {@code elementType} flows. For the plain variant that is an opaque
     * {@link QueriedValue} standing for the element. For the {@code WITH ORDINALITY} variant it is a
     * {@link RecordConstructorValue} of two such values, the element and the ordinal, rather than a single opaque value
     * of the struct type.
     *
     * @param elementType the element type of the collection being exploded
     * @param withOrdinality whether ordinals are produced alongside the elements
     * @return the value flowed by such an explode
     */
    @Nonnull
    public static Value explodeResultValue(@Nonnull final Type elementType, final boolean withOrdinality) {
        final var elementValue = new QueriedValue(elementType);
        if (!withOrdinality) {
            return elementValue;
        }
        // Note: the element must stay the first column. `MaxMatchMap` returns the first reachable candidate value that
        // compares equal, and a `QueriedValue` has no identity beyond its class and result type, so an element whose
        // type is also a non-nullable `INT` -- the ordinal's type -- would just as happily match the ordinal if the
        // ordinal came first.
        //
        // The record is built nullable because `explodeResultType` declares it that way, and the constructor verifies
        // that the two agree: that declared type is what the plan looks its protobuf descriptor up by at run time.
        return RecordConstructorValue.ofColumns(
                ImmutableList.of(Column.unnamedOf(elementValue),
                        Column.unnamedOf(new QueriedValue(Type.primitiveType(Type.TypeCode.INT, false)))),
                true);
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
            return new ExplodeExpression(translatedCollectionValue, withOrdinality, zeroBasedOrdinality);
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

    /**
     * Establishes that an explode <em>without</em> ordinality is subsumed by an explode <em>with</em> ordinality over
     * the same collection. The candidate emits one {@code (element, ordinal)} struct per element this expression
     * emits just element. That satisfies subsumption: the candidate produces at least everything the query may produce.
     * This case cannot be dealt with by {@link #exactlySubsumedBy}, whose {@code equalsWithoutChildren} compares
     * {@link #isWithOrdinality()}.
     *
     * @param candidateExpression the candidate explode, which must be {@code WITH ORDINALITY}
     * @param bindingAliasMap a map of aliases defining the equivalence between quantifiers
     * @param partialMatchMap a map from quantifier to the {@link PartialMatch} pulled up along that quantifier
     * @return an iterable containing a {@link MatchInfo} if subsumption holds, empty otherwise
     */
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

    public static ExplodeExpression explodeField(@Nonnull final Quantifier.ForEach baseQuantifier,
                                                 @Nonnull final List<String> fieldNames) {
        return new ExplodeExpression(FieldValue.ofFieldNames(baseQuantifier.getFlowedObjectValue(), fieldNames));
    }
}
