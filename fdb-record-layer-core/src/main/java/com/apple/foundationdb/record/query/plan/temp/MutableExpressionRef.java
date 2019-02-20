/*
 * MutableExpressionRef.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * A <code>MutableExpressionRef</code> is a reference to one or more {@link PlannerExpression}s. It supports inserting a
 * new planner expression into the reference, which has very general semantics.
 *
 * For example, {@link SingleExpressionRef} represents a single boxed expression, so {@link #insert(PlannerExpression)}
 * replaces the current expression with the given new one.
 * @param <T> the type of planner expression that is contained in this reference
 */
@API(API.Status.EXPERIMENTAL)
public interface MutableExpressionRef<T extends PlannerExpression> extends ExpressionRef<T> {
    void insert(@Nonnull T newValue);
}
