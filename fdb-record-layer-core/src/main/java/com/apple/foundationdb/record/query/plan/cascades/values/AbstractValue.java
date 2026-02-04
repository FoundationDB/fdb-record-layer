/*
 * AbstractValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Abstract implementation of {@link Value} that provides memoization of correlatedTo sets, and other computations.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AbstractValue implements Value {

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlatedToSupplier;

    @Nonnull
    private final Supplier<Integer> semanticHashCodeSupplier;

    @Nonnull
    private final Supplier<Integer> heightSupplier;

    @Nonnull
    private final Supplier<Iterable<? extends Value>> childrenSupplier;

    @Nonnull
    private final Supplier<Boolean> isIndexOnlySupplier;

    @SuppressWarnings("this-escape")
    protected AbstractValue() {
        this.correlatedToSupplier = Suppliers.memoize(this::computeCorrelatedTo);
        this.semanticHashCodeSupplier = Suppliers.memoize(this::computeSemanticHashCode);
        this.heightSupplier = Suppliers.memoize(Value.super::height);
        this.childrenSupplier = Suppliers.memoize(this::computeChildren);
        this.isIndexOnlySupplier = Suppliers.memoize(this::computeIsIndexOnly);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return correlatedToSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedTo() {
        return fold(Value::getCorrelatedToWithoutChildren,
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

    private int computeSemanticHashCode() {
        return fold(Value::hashCodeWithoutChildren,
                (hashCodeWithoutChildren, childrenHashCodes) -> Objects.hash(childrenHashCodes, hashCodeWithoutChildren));
    }

    @Override
    public int height() {
        return heightSupplier.get();
    }

    private boolean computeIsIndexOnly() {
        return preOrderStream().anyMatch(IndexOnlyValue.class::isInstance);
    }

    @Override
    public boolean isIndexOnly() {
        return isIndexOnlySupplier.get();
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return childrenSupplier.get();
    }

    @Nonnull
    protected abstract Iterable<? extends Value> computeChildren();

    @Override
    public String toString() {
        return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
    }
}
