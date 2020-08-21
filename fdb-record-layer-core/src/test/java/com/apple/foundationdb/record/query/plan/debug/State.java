/*
 * State.java
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

package com.apple.foundationdb.record.query.plan.debug;

import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.List;

class State {
    private int currentExpressionIndex;
    private int currentReferenceIndex;
    private int currentQuantifierIndex;

    @Nonnull
    private final Cache<Integer, RelationalExpression> expressionCache;
    @Nonnull private final Cache<RelationalExpression, Integer> invertedExpressionsCache;
    @Nonnull private final Cache<Integer, ExpressionRef<? extends RelationalExpression>> referenceCache;
    @Nonnull private final Cache<ExpressionRef<? extends RelationalExpression>, Integer> invertedReferenceCache;
    @Nonnull private final Cache<Integer, Quantifier> quantifierCache;
    @Nonnull private final Cache<Quantifier, Integer> invertedQuantifierCache;

    @Nonnull private final List<Debugger.Event> events;

    private int currentTick;

    public static State initial() {
        return new State();
    }

    public static State copyOf(final State source) {
        final Cache<Integer, RelationalExpression> copyExpressionCache = CacheBuilder.newBuilder().weakValues().build();
        source.getExpressionCache().asMap().forEach(copyExpressionCache::put);
        final Cache<RelationalExpression, Integer> copyInvertedExpressionsCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedExpressionsCache().asMap().forEach(copyInvertedExpressionsCache::put);
        final Cache<Integer, ExpressionRef<? extends RelationalExpression>> copyReferenceCache = CacheBuilder.newBuilder().weakValues().build();
        source.getReferenceCache().asMap().forEach(copyReferenceCache::put);
        final Cache<ExpressionRef<? extends RelationalExpression>, Integer> copyInvertedReferenceCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedReferenceCache().asMap().forEach(copyInvertedReferenceCache::put);
        final Cache<Integer, Quantifier> copyQuantifierCache = CacheBuilder.newBuilder().weakValues().build();
        source.getQuantifierCache().asMap().forEach(copyQuantifierCache::put);
        final Cache<Quantifier, Integer> copyInvertedQuantifierCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedQuantifierCache().asMap().forEach(copyInvertedQuantifierCache::put);

        return new State(source.getCurrentExpressionIndex(),
                source.getCurrentReferenceIndex(),
                source.getCurrentQuantifierIndex(),
                copyExpressionCache,
                copyInvertedExpressionsCache,
                copyReferenceCache,
                copyInvertedReferenceCache,
                copyQuantifierCache,
                copyInvertedQuantifierCache,
                Lists.newArrayList(source.getEvents()),
                source.getCurrentTick());
    }

    private State() {
        this(0,
                0,
                0,
                CacheBuilder.newBuilder().weakValues().build(),
                CacheBuilder.newBuilder().weakKeys().build(),
                CacheBuilder.newBuilder().weakValues().build(),
                CacheBuilder.newBuilder().weakKeys().build(),
                CacheBuilder.newBuilder().weakValues().build(),
                CacheBuilder.newBuilder().weakKeys().build(),
                Lists.newArrayList(),
                -1);
    }

    private State(final int currentExpressionIndex,
                  final int currentReferenceIndex,
                  final int currentQuantifierIndex,
                  @Nonnull final Cache<Integer, RelationalExpression> expressionCache,
                  @Nonnull final Cache<RelationalExpression, Integer> invertedExpressionsCache,
                  @Nonnull final Cache<Integer, ExpressionRef<? extends RelationalExpression>> referenceCache,
                  @Nonnull final Cache<ExpressionRef<? extends RelationalExpression>, Integer> invertedReferenceCache,
                  @Nonnull final Cache<Integer, Quantifier> quantifierCache,
                  @Nonnull final Cache<Quantifier, Integer> invertedQuantifierCache,
                  @Nonnull final List<Debugger.Event> events,
                  @Nonnull final int currentTick) {
        this.currentExpressionIndex = currentExpressionIndex;
        this.currentReferenceIndex = currentReferenceIndex;
        this.currentQuantifierIndex = currentQuantifierIndex;
        this.expressionCache = expressionCache;
        this.invertedExpressionsCache = invertedExpressionsCache;
        this.referenceCache = referenceCache;
        this.invertedReferenceCache = invertedReferenceCache;
        this.quantifierCache = quantifierCache;
        this.invertedQuantifierCache = invertedQuantifierCache;
        this.events = events;
        this.currentTick = currentTick;
    }

    public int getCurrentExpressionIndex() {
        return currentExpressionIndex;
    }

    public int getCurrentReferenceIndex() {
        return currentReferenceIndex;
    }

    public int getCurrentQuantifierIndex() {
        return currentQuantifierIndex;
    }

    @Nonnull
    public Cache<Integer, RelationalExpression> getExpressionCache() {
        return expressionCache;
    }

    @Nonnull
    public Cache<RelationalExpression, Integer> getInvertedExpressionsCache() {
        return invertedExpressionsCache;
    }

    @Nonnull
    public Cache<Integer, ExpressionRef<? extends RelationalExpression>> getReferenceCache() {
        return referenceCache;
    }

    @Nonnull
    public Cache<ExpressionRef<? extends RelationalExpression>, Integer> getInvertedReferenceCache() {
        return invertedReferenceCache;
    }

    @Nonnull
    public Cache<Integer, Quantifier> getQuantifierCache() {
        return quantifierCache;
    }

    @Nonnull
    public Cache<Quantifier, Integer> getInvertedQuantifierCache() {
        return invertedQuantifierCache;
    }

    @Nonnull
    public List<Debugger.Event> getEvents() {
        return events;
    }

    public int getCurrentTick() {
        return currentTick;
    }

    public void registerExpression(final RelationalExpression expression) {
        if (invertedExpressionsCache.getIfPresent(expression) == null) {
            expressionCache.put(currentExpressionIndex, expression);
            invertedExpressionsCache.put(expression, currentExpressionIndex);
            currentExpressionIndex ++;
        }
    }

    public void registerReference(final ExpressionRef<? extends RelationalExpression> reference) {
        if (invertedReferenceCache.getIfPresent(reference) == null) {
            referenceCache.put(currentReferenceIndex, reference);
            invertedReferenceCache.put(reference, currentReferenceIndex);
            currentReferenceIndex ++;
        }
    }

    public void registerQuantifier(final Quantifier quantifier) {
        if (invertedQuantifierCache.getIfPresent(quantifier) == null) {
            quantifierCache.put(currentQuantifierIndex, quantifier);
            invertedQuantifierCache.put(quantifier, currentQuantifierIndex);
            currentQuantifierIndex ++;
        }
    }

    public void addCurrentEvent(@Nonnull final Debugger.Event event) {
        events.add(event);
        currentTick = events.size() - 1;
    }
}
