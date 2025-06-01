/*
 * ExpressionPartition.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A partition used for matching holding a set of {@link RelationalExpression}s.
 * @param <E> type parameter to indicate the type of expression this partition holds.
 */
public class ExpressionPartition<E extends RelationalExpression> {
    @Nonnull
    private final Map<ExpressionProperty<?>, ?> groupingPropertyMap;
    @Nonnull
    private final Map<E, Map<ExpressionProperty<?>, ?>> groupedPropertyMap;

    public ExpressionPartition(@Nonnull final Map<ExpressionProperty<?>, ?> groupingPropertyMap,
                               @Nonnull final Map<E, Map<ExpressionProperty<?>, ?>> groupedPropertyMap) {
        this.groupingPropertyMap = groupingPropertyMap;
        this.groupedPropertyMap = groupedPropertyMap;
    }

    @Nonnull
    public Map<ExpressionProperty<?>, ?> getGroupingPropertyMap() {
        return groupingPropertyMap;
    }

    @Nonnull
    public Map<E, Map<ExpressionProperty<?>, ?>> getGroupedPropertyMap() {
        return groupedPropertyMap;
    }

    @Nonnull
    public <A> A getPropertyValue(@Nonnull final ExpressionProperty<A> expressionProperty) {
        return expressionProperty.narrowAttribute(Objects.requireNonNull(groupingPropertyMap.get(expressionProperty)));
    }

    @Nonnull
    public Set<E> getExpressions() {
        return groupedPropertyMap.keySet();
    }
}
