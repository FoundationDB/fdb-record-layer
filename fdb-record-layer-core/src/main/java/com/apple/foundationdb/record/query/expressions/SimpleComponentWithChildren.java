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
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.List;

@API(API.Status.INTERNAL)
class SimpleComponentWithChildren {
    /**
     * Children for this component, at least 2 of them.
     */
    @Nonnull
    private final List<QueryComponent> children;

    /**
     * Creates a new component with children, must have at least 2 children. The planner assumes this.
     * @param children the operands
     */
    protected SimpleComponentWithChildren(@Nonnull List<QueryComponent> children) {
        if (children.size() < 2) {
            throw new RecordCoreException(getClass().getSimpleName() + " must have at least two children");
        }
        this.children = children;
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
        return children;
    }
}
