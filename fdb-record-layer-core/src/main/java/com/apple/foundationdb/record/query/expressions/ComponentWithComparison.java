/*
 * ComponentWithComparison.java
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

import javax.annotation.Nonnull;

/**
 * A {@link QueryComponent} that uses a {@link Comparisons.Comparison} on the record.
 */
@API(API.Status.MAINTAINED)
public interface ComponentWithComparison extends ComponentWithNoChildren {
    @Nonnull
    Comparisons.Comparison getComparison();

    QueryComponent withOtherComparison(Comparisons.Comparison comparison);

    String getName();

    @Nonnull
    @Override
    default QueryComponent withParameterRelationshipMap(@Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        return withOtherComparison(getComparison().withParameterRelationshipMap(parameterRelationshipGraph));
    }
}
