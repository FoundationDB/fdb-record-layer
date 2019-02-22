/*
 * SimpleComponentWithChildren.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@API(API.Status.INTERNAL)
class SimpleComponentWithChildren implements PlannerExpression {
    /**
     * Children for this component, at least 2 of them.
     */
    @Nonnull
    private final List<ExpressionRef<QueryComponent>> children;
    @Nonnull
    private final List<ExpressionRef<? extends PlannerExpression>> childrenRefView;

    /**
     * Creates a new component with children, must have at least 2 children. The planner assumes this.
     * @param children the operands
     */
    SimpleComponentWithChildren(@Nonnull List<ExpressionRef<QueryComponent>> children) {
        if (children.size() < 2) {
            throw new RecordCoreException(getClass().getSimpleName() + " must have at least two children");
        }
        this.children = children;
        this.childrenRefView = new ArrayList<>();
        this.childrenRefView.addAll(this.children);
    }

    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        for (QueryComponent child : getChildren()) {
            child.validate(descriptor);
        }
    }

    /**
     * Children for this component, at least 2 of them.
     * @return the children of this component
     */
    @Nonnull
    public List<QueryComponent> getChildren() {
        return children.stream().map(ExpressionRef::get).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return childrenRefView.iterator();
    }
}
