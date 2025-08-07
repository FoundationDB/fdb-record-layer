/*
 * SymbolState.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CanIgnoreReturnValue;



import javax.annotation.Nonnull;
import java.util.Map;
import java.util.function.IntUnaryOperator;

public class SymbolTables {
    @Nonnull
    private final Map<Class<?>, Integer> classToIndexMap;
    @Nonnull
    private final Cache<Integer, RelationalExpression> expressionCache;
    @Nonnull private final Cache<RelationalExpression, Integer> invertedExpressionsCache;
    @Nonnull private final Cache<Integer, Reference> referenceCache;
    @Nonnull private final Cache<Reference, Integer> invertedReferenceCache;
    @Nonnull private final Cache<Integer, Quantifier> quantifierCache;
    @Nonnull private final Cache<Quantifier, Integer> invertedQuantifierCache;

    public static SymbolTables copyOf(final SymbolTables source) {
        final Cache<Integer, RelationalExpression> copyExpressionCache = CacheBuilder.newBuilder().weakValues().build();
        source.getExpressionCache().asMap().forEach(copyExpressionCache::put);
        final Cache<RelationalExpression, Integer> copyInvertedExpressionsCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedExpressionsCache().asMap().forEach(copyInvertedExpressionsCache::put);
        final Cache<Integer, Reference> copyReferenceCache = CacheBuilder.newBuilder().weakValues().build();
        source.getReferenceCache().asMap().forEach(copyReferenceCache::put);
        final Cache<Reference, Integer> copyInvertedReferenceCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedReferenceCache().asMap().forEach(copyInvertedReferenceCache::put);
        final Cache<Integer, Quantifier> copyQuantifierCache = CacheBuilder.newBuilder().weakValues().build();
        source.getQuantifierCache().asMap().forEach(copyQuantifierCache::put);
        final Cache<Quantifier, Integer> copyInvertedQuantifierCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedQuantifierCache().asMap().forEach(copyInvertedQuantifierCache::put);

        return new SymbolTables(source.getClassToIndexMap(),
                copyExpressionCache,
                copyInvertedExpressionsCache,
                copyReferenceCache,
                copyInvertedReferenceCache,
                copyQuantifierCache,
                copyInvertedQuantifierCache);
    }

    public SymbolTables() {
        classToIndexMap = Maps.newHashMap();
        expressionCache = CacheBuilder.newBuilder().weakValues().build();
        invertedExpressionsCache =  CacheBuilder.newBuilder().weakKeys().build();
        referenceCache = CacheBuilder.newBuilder().weakValues().build();
        invertedReferenceCache = CacheBuilder.newBuilder().weakKeys().build();
        quantifierCache = CacheBuilder.newBuilder().weakValues().build();
        invertedQuantifierCache = CacheBuilder.newBuilder().weakKeys().build();
    }

    private SymbolTables(@Nonnull final Map<Class<?>, Integer> classToIndexMap,
                         @Nonnull final Cache<Integer, RelationalExpression> expressionCache,
                         @Nonnull final Cache<RelationalExpression, Integer> invertedExpressionsCache,
                         @Nonnull final Cache<Integer, Reference> referenceCache,
                         @Nonnull final Cache<Reference, Integer> invertedReferenceCache,
                         @Nonnull final Cache<Integer, Quantifier> quantifierCache,
                         @Nonnull final Cache<Quantifier, Integer> invertedQuantifierCache) {
        this.classToIndexMap = Maps.newHashMap(classToIndexMap);
        this.expressionCache = expressionCache;
        this.invertedExpressionsCache = invertedExpressionsCache;
        this.referenceCache = referenceCache;
        this.invertedReferenceCache = invertedReferenceCache;
        this.quantifierCache = quantifierCache;
        this.invertedQuantifierCache = invertedQuantifierCache;
    }

    @Nonnull
    private Map<Class<?>, Integer> getClassToIndexMap() {
        return classToIndexMap;
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
    public Cache<Integer, Reference> getReferenceCache() {
        return referenceCache;
    }

    @Nonnull
    public Cache<Reference, Integer> getInvertedReferenceCache() {
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


    public int getIndex(final Class<?> clazz) {
        return classToIndexMap.getOrDefault(clazz, 0);
    }

    @CanIgnoreReturnValue
    public int updateIndex(final Class<?> clazz, IntUnaryOperator computeFn) {
        return classToIndexMap.compute(clazz, (c, value) -> value == null ? computeFn.applyAsInt(0) : computeFn.applyAsInt(value));
    }

    public void registerExpression(final RelationalExpression expression) {
        if (invertedExpressionsCache.getIfPresent(expression) == null) {
            final int index = getIndex(RelationalExpression.class);
            expressionCache.put(index, expression);
            invertedExpressionsCache.put(expression, index);
            updateIndex(RelationalExpression.class, i -> i + 1);
        }
    }

    public void registerReference(final Reference reference) {
        if (invertedReferenceCache.getIfPresent(reference) == null) {
            final int index = getIndex(Reference.class);
            referenceCache.put(index, reference);
            invertedReferenceCache.put(reference, index);
            updateIndex(Reference.class, i -> i + 1);
        }
    }

    public void registerQuantifier(final Quantifier quantifier) {
        if (invertedQuantifierCache.getIfPresent(quantifier) == null) {
            final int index = getIndex(Quantifier.class);
            quantifierCache.put(index, quantifier);
            invertedQuantifierCache.put(quantifier, index);
            updateIndex(Quantifier.class, i -> i + 1);
        }
    }

}
