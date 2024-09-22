/*
 * PullUp.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Chain to pull up {@link Value} trees through a series of relational expressions.
 */
public class PullUp {
    @Nonnull
    final Value pullThroughValue;

    @Nonnull
    final Set<CorrelationIdentifier> constantAliases;

    private PullUp(@Nonnull final Value pullThroughValue, @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        this.pullThroughValue = pullThroughValue;
        this.constantAliases = ImmutableSet.copyOf(constantAliases);
    }

    @Nonnull
    public PullUp nest(@Nonnull final CorrelationIdentifier nestingAlias,
                       @Nonnull final Value lowerPullThroughValue,
                       @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var completePullThroughValue =
                pullThroughValue.translateCorrelationsAndSimplify(
                        TranslationMap.builder()
                                .when(nestingAlias).then(((sourceAlias, leafValue) -> lowerPullThroughValue))
                                .build());
        return new PullUp(completePullThroughValue, constantAliases);
    }

    @Nonnull
    public PullUp nest(@Nonnull final CorrelationIdentifier nestingAlias,
                       @Nonnull final CorrelationIdentifier lowerAlias,
                       @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var completePullThroughValue =
                pullThroughValue.translateCorrelations(
                        TranslationMap.builder()
                                .when(nestingAlias).then(((sourceAlias, leafValue) -> {
                                    if (leafValue instanceof QuantifiedObjectValue) {
                                        return QuantifiedObjectValue.of(lowerAlias, leafValue.getResultType());
                                    }
                                    throw new RecordCoreException("unexpected correlated leaf value");
                                }))
                                .build());
        return new PullUp(completePullThroughValue, constantAliases);
    }

    @Nonnull
    public Optional<Value> pullUp(@Nonnull final Value value, @Nonnull final CorrelationIdentifier upperBaseAlias) {
        final var pullUpMap =
                pullThroughValue.pullUp(ImmutableList.of(value),
                        AliasMap.emptyMap(),
                        constantAliases,
                        upperBaseAlias);
        return Optional.ofNullable(pullUpMap.get(value));
    }

    @Nonnull
    public Map<Value, Value> pullUp(@Nonnull final List<Value> values, @Nonnull final CorrelationIdentifier upperBaseAlias) {
        return pullThroughValue.pullUp(values,
                AliasMap.emptyMap(),
                constantAliases,
                upperBaseAlias);
    }

    @Nonnull
    public static PullUp initial(@Nonnull final Value pullThroughValue) {
        return new PullUp(pullThroughValue, ImmutableSet.of());
    }
}
