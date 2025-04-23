/*
 * ExpressionProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;

import javax.annotation.Nonnull;

/**
 * Base interface to capture attributes for expressions.
 * An instance of this usually class serves as a key in maps much like an enum, but provides strong typing.
 * @param <P> the type representing the actual property
 */
public interface ExpressionProperty<P> {
    /**
     * Method to narrow the type from {@link Object} to the declared type of the attribute. Note that
     * the caller must guarantee that the narrowing is well-defined and successful.
     * @param object an object that actually is of dynamic type {@code P}
     * @return the narrowed object of type {@code P}
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default P narrowAttribute(@Nonnull final Object object) {
        return (P)object;
    }

    @Nonnull
    RelationalExpressionVisitor<P> createVisitor();

    @Nonnull
    static <T> RelationalExpressionVisitor<T> toExpressionVisitor(@Nonnull final RecordQueryPlanVisitor<T> planVisitor) {
        return new RelationalExpressionVisitorWithDefaults<>() {
            @Nonnull
            @Override
            public T visitDefault(@Nonnull final RelationalExpression element) {
                if (element instanceof RecordQueryPlan) {
                    return planVisitor.visit((RecordQueryPlan)element);
                }
                throw new UnsupportedOperationException("plan visitor does not support visitor interface for ordinary expressions");
            }
        };
    }
}
