/*
 * RecursiveUnionExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This is a logical representation of a recursive union, a recursive union is similar to a normal unordered union, however
 * its legs have special execution semantics; just like a union, it returns the results verbatim of one particular
 * leg called the "initial state" leg. This leg provides the results required to seed the recursion happening during
 * the execution of the other leg, the "recursive state" leg. The recursive unions repeatedly executes the recursive
 * leg until it does not produce any more results (fix-point).
 */
public class RecursiveUnionExpression extends AbstractRelationalExpressionWithChildren {

    @Nonnull
    private final Quantifier initialStateQuantifier;

    @Nonnull
    private final Quantifier recursiveStateQuantifier;

    @Nonnull
    private final CorrelationIdentifier tempTableScanAlias;

    @Nonnull
    private final CorrelationIdentifier tempTableInsertAlias;

    @Nonnull
    private final Value resultValue;

    public RecursiveUnionExpression(@Nonnull final Quantifier initialState,
                                    @Nonnull final Quantifier recursiveState,
                                    @Nonnull final CorrelationIdentifier tempTableScanAlias,
                                    @Nonnull final CorrelationIdentifier tempTableInsertAlias) {
        this.initialStateQuantifier = initialState;
        this.recursiveStateQuantifier = recursiveState;
        this.tempTableScanAlias = tempTableScanAlias;
        this.tempTableInsertAlias = tempTableInsertAlias;
        this.resultValue = RecordQuerySetPlan.mergeValues(ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier));
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        Streams.concat(initialStateQuantifier.getCorrelatedTo().stream(),
                        recursiveStateQuantifier.getCorrelatedTo().stream())
                // filter out the correlations that are satisfied by this plan
                .filter(alias -> !alias.equals(tempTableInsertAlias) && !alias.equals(tempTableScanAlias))
                .forEach(builder::add);
        return builder.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecursiveUnionExpression)) {
            return false;
        }
        final var otherRecursiveUnionExpression = (RecursiveUnionExpression)otherExpression;
        return (tempTableScanAlias.equals(otherRecursiveUnionExpression.tempTableScanAlias)
                        || equivalences.containsMapping(tempTableScanAlias, otherRecursiveUnionExpression.tempTableScanAlias)) &&
                (tempTableInsertAlias.equals(otherRecursiveUnionExpression.tempTableInsertAlias)
                         || equivalences.containsMapping(tempTableInsertAlias, otherRecursiveUnionExpression.tempTableInsertAlias));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getTempTableScanAlias(), getTempTableInsertAlias());
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        Verify.verify(!translationMap.containsSourceAlias(tempTableScanAlias)
                && !translationMap.containsSourceAlias(tempTableInsertAlias));
        final var translatedInitialStateQun = translatedQuantifiers.get(0);
        final var translatedRecursiveStateQun = translatedQuantifiers.get(1);
        return new RecursiveUnionExpression(translatedInitialStateQun, translatedRecursiveStateQun,
                tempTableScanAlias, tempTableInsertAlias);
    }

    @Nonnull
    public CorrelationIdentifier getTempTableScanAlias() {
        return tempTableScanAlias;
    }

    @Nonnull
    public CorrelationIdentifier getTempTableInsertAlias() {
        return tempTableInsertAlias;
    }

    @Nonnull
    public Quantifier getInitialStateQuantifier() {
        return initialStateQuantifier;
    }

    @Nonnull
    public Quantifier getRecursiveStateQuantifier() {
        return recursiveStateQuantifier;
    }
}
