/*
 * Attribute.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.explain;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Basic class for all attributes of {@link AbstractPlannerGraph.AbstractNode}
 * as well as {@link AbstractPlannerGraph.AbstractEdge}.
 *
 * Represents a tag object that annotates an {@link AbstractPlannerGraph.AbstractNode} or an
 * {@link AbstractPlannerGraph.AbstractEdge}, providing additional information to a {@link GraphExporter}.
 */
public abstract class Attribute {
    @Nonnull
    private final Object reference;

    protected Attribute(final Object reference) {
        this.reference = reference;
    }

    /**
     * Returns whether this method is semantic of it contains visual cues of other non-semantic information.
     *
     * @return {@code true} if the attribute is semantic, {@code false} otherwise.
     */
    public abstract boolean isSemanticAttribute();

    /**
     * This method returns the underlying object this attribute refers to.
     * @return the object.
     */
    @Nullable
    public Object getReference() {
        return reference;
    }

    @Override
    public String toString() {
        return Objects.requireNonNull(reference).toString();
    }
}
