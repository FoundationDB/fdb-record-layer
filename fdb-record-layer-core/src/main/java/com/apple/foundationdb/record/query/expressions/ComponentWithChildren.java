/*
 * ComponentWithChildren.java
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
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A {@link QueryComponent} that has child components.
 */
@API(API.Status.UNSTABLE)
public interface ComponentWithChildren extends QueryComponent {
    @Nonnull
    List<QueryComponent> getChildren();

    QueryComponent withOtherChildren(List<QueryComponent> newChildren);

    @Nonnull
    @Override
    default QueryComponent withParameterRelationshipMap(@Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        return withOtherChildren(
                getChildren()
                        .stream()
                        .map(child -> child.withParameterRelationshipMap(parameterRelationshipGraph))
                        .collect(ImmutableList.toImmutableList()));
    }
}
