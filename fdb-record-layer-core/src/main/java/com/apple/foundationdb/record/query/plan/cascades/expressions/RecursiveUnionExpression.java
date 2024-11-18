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
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * TODO.
 */
public class RecursiveUnionExpression implements RelationalExpressionWithChildren {

    @Nonnull
    private final List<Quantifier> quantifiers;

    @Nonnull
    private final Value initialTempTableValueReference;

    @Nonnull
    private final Value recursiveTempTableValueReference;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlationsSupplier;

    public RecursiveUnionExpression(@Nonnull final Quantifier initialState,
                                    @Nonnull final Quantifier recursiveState,
                                    @Nonnull final Value initialTempTableValueReference,
                                    @Nonnull final Value recursiveTempTableValueReference) {
        this.quantifiers = ImmutableList.of(initialState, recursiveState);
        this.initialTempTableValueReference = initialTempTableValueReference;
        this.recursiveTempTableValueReference = recursiveTempTableValueReference;
        this.resultValue = RecordQuerySetPlan.mergeValues(quantifiers);
        this.correlationsSupplier = Suppliers.memoize(this::computeCorrelatedTo);
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return correlationsSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedTo() {
        return ImmutableSet.<CorrelationIdentifier>builder()
                .addAll(getInitialTempTableValueReference().getCorrelatedTo())
                .addAll(getRecursiveTempTableValueReference().getCorrelatedTo())
                .build();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecursiveUnionExpression)) {
            return false;
        }

        final var otherRecursiveUnionExpression = (RecursiveUnionExpression)otherExpression;

        return getInitialTempTableValueReference().semanticEquals(otherRecursiveUnionExpression.getInitialTempTableValueReference(), equivalences)
                && getRecursiveTempTableValueReference().semanticEquals(otherRecursiveUnionExpression.getRecursiveTempTableValueReference(), equivalences);
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
    public int hashCodeWithoutChildren() {
        return Objects.hash(getInitialTempTableValueReference(), getRecursiveTempTableValueReference());
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        final var translatedInitialTempTableValueReference = getInitialTempTableValueReference().translateCorrelations(translationMap);
        final var translatedRecursiveTempTableValueReference = getRecursiveTempTableValueReference().translateCorrelations(translationMap);
        if (translatedInitialTempTableValueReference != getInitialTempTableValueReference()
                || translatedRecursiveTempTableValueReference != getRecursiveTempTableValueReference()) {
            return new RecursiveUnionExpression(translatedQuantifiers.get(0), translatedQuantifiers.get(1),
                    getInitialTempTableValueReference().translateCorrelations(translationMap),
                    getRecursiveTempTableValueReference().translateCorrelations(translationMap));
        }
        return this;
    }

    @Nonnull
    public Value getInitialTempTableValueReference() {
        return initialTempTableValueReference;
    }

    @Nonnull
    public Value getRecursiveTempTableValueReference() {
        return recursiveTempTableValueReference;
    }
}
