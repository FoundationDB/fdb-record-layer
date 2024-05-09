/*
 * PlanVisitorHelpers.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitor;

import javax.annotation.Nonnull;

/**
 * Helper class for creating and transforming plan visitors.
 */
public class PlanVisitorHelpers {
    private PlanVisitorHelpers() {
        // prevent instantiation
    }

    @Nonnull
    public static <T> RelationalExpressionVisitor<T> toExpressionVisitor(@Nonnull final RecordQueryPlanVisitor<T> planVisitor) {
        return new RelationalExpressionVisitorWithDefaults<T>() {
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
