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
 * Base interface to capture properties for expressions.
 * An instance of this usually class serves as a key in maps much like an enum, but provides strong typing.
 * In particular, the following holds true for all properties:
 * <ul>
 *     <li>
 *         Properties are singletons (or multitons; enumerated singletons) of implementors of this class.
 *     </li>
 *     <li>
 *         Properties create a {@link RelationalExpressionVisitor} using the method {@link #createVisitor()}}. That
 *         means that properties use visitors that operate on all kinds of relational expressions (from a Java typing
 *         standpoint). In reality, as some properties are meaningless for some expressions but not others, the visitor
 *         may decide to throw an exception for a class it cannot process.
 *         There are some helpers that can transform e.g. a {@link RecordQueryPlanVisitor} into a
 *         {@link RelationalExpressionVisitor} which add lowing to throw an exception when a non-plan is visited. This
 *         does not weaken the usual contracts that these visitor classes have. It just allows for e.g.
 *         {@link RecordQueryPlanVisitor}s to be used in place of {@link RelationalExpressionVisitor}s.
 *     </li>
 *     <li>
 *         Properties are not dependent on parameters but is computable solely by traversing the
 *         {@link RelationalExpression} DAG. The method {@link #createVisitor()} does not use any parameters.
 *     </li>
 *     <li>
 *         Properties may or may not be used in an {@link ExpressionPropertiesMap}. If the property is maintained in
 *         such a map for a given reference, the implementor of the visitor may assume that the property values for
 *         children expressions are also maintained in such a map and therefore accessible though the individual
 *         child's {@link Reference}. In this way, expensive re-computation of the property value can be avoided.
 *     </li>
 * </ul>
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
