/*
 * PlanPartition.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A partition used for matching holding a set of {@link RelationalExpression}s.
 * @param <E> type parameter to indicate the type of expression this partition holds.
 */
public class ExpressionPartition<E extends RelationalExpression> {
    @Nonnull
    private final Map<ExpressionProperty<?>, ?> propertyValuesMap;
    @Nonnull
    private final Set<E> expressions;

    public ExpressionPartition(final Map<ExpressionProperty<?>, ?> propertyValuesMap, final Collection<E> expressions) {
        this.propertyValuesMap = ImmutableMap.copyOf(propertyValuesMap);
        this.expressions = new LinkedIdentitySet<>(expressions);
    }

    @Nonnull
    public Map<ExpressionProperty<?>, ?> getPropertyValuesMap() {
        return propertyValuesMap;
    }

    @Nonnull
    public <A> A getPropertyValue(@Nonnull final ExpressionProperty<A> expressionProperty) {
        return expressionProperty.narrowAttribute(Objects.requireNonNull(propertyValuesMap.get(expressionProperty)));
    }

    @Nonnull
    public Set<E> getExpressions() {
        return expressions;
    }
}
