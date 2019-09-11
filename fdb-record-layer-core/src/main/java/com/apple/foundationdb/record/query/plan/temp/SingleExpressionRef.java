/*
 * SingleExpressionRef.java
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
 * A mutable reference to a single planner expression. Since it references a single expression, it can provide
 * access to it via the {@link #get()} method.
 * @param <T> the type of planner expression that is contained in this reference
 */
@API(API.Status.EXPERIMENTAL)
public class SingleExpressionRef<T extends PlannerExpression> extends GroupExpressionRef<T> {
    protected SingleExpressionRef(@Nonnull T expression) {
        super(expression);
    }

    public static <T extends PlannerExpression> SingleExpressionRef<T> of(@Nonnull T expression) {
        return new SingleExpressionRef<>(expression);
    }
}
