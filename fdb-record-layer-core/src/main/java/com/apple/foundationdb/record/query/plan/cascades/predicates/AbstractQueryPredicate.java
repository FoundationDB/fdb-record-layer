/*
 * AbstractQueryPredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PAbstractQueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Abstract implementation of {@link QueryPredicate} that provides memoization of correlatedTo sets.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AbstractQueryPredicate implements QueryPredicate {

    private final boolean isAtomic;

    private final Supplier<Set<CorrelationIdentifier>> correlatedToSupplier;

    private final Supplier<Integer> semanticHashCodeSupplier;

    private final Supplier<Integer> heightSupplier;

    @SuppressWarnings("unused")
    protected AbstractQueryPredicate(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PAbstractQueryPredicate abstractQueryPredicateProto) {
        this(PlanSerialization.getFieldOrThrow(abstractQueryPredicateProto,
                PAbstractQueryPredicate::hasIsAtomic, PAbstractQueryPredicate::getIsAtomic));
    }

    protected AbstractQueryPredicate(final boolean isAtomic) {
        this.isAtomic = isAtomic;
        this.correlatedToSupplier = Suppliers.memoize(this::computeCorrelatedTo);
        this.semanticHashCodeSupplier = Suppliers.memoize(this::computeSemanticHashCode);
        this.heightSupplier = Suppliers.memoize(QueryPredicate.super::height);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return correlatedToSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedTo() {
        return fold(QueryPredicate::getCorrelatedToWithoutChildren,
                (correlatedToWithoutChildren, childrenCorrelatedTo) -> {
                    ImmutableSet.Builder<CorrelationIdentifier> correlatedToBuilder = ImmutableSet.builder();
                    correlatedToBuilder.addAll(correlatedToWithoutChildren);
                    childrenCorrelatedTo.forEach(correlatedToBuilder::addAll);
                    return correlatedToBuilder.build();
                });
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public int semanticHashCode() {
        return semanticHashCodeSupplier.get();
    }

    protected abstract int computeSemanticHashCode();

    @Override
    public int height() {
        return heightSupplier.get();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(isAtomic);
    }

    @Override
    public boolean isAtomic() {
        return isAtomic;
    }

    @Nonnull
    public PAbstractQueryPredicate toAbstractQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PAbstractQueryPredicate.newBuilder().setIsAtomic(isAtomic).build();
    }
}
