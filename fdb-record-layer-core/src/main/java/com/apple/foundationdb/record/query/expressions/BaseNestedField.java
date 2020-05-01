/*
 * BaseNestedField.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * An abstract base class for all {@link QueryComponent}s that represent a query of a nested record type.
 */
@API(API.Status.INTERNAL)
public abstract class BaseNestedField extends BaseField implements ComponentWithSingleChild {
    @Nonnull
    protected final ExpressionRef<QueryComponent> childComponent;

    public BaseNestedField(String fieldName, @Nonnull ExpressionRef<QueryComponent> childComponent) {
        super(fieldName);
        this.childComponent = childComponent;
    }

    @Override
    @Nonnull
    public QueryComponent getChild() {
        return childComponent.get();
    }

    @Override
    public abstract QueryComponent withOtherChild(QueryComponent newChild);

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(this.childComponent);
    }

    @Override
    public boolean isAsync() {
        return getChild().isAsync();
    }
}
