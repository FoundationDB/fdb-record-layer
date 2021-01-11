/*
 * KeyExpressionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A sub interface of {@link KeyExpressionVisitor} that fixes the return type to be a {@link GraphExpansion} and
 * adds an API to cause the expansion of a metadata-based data access structure to a data flow graph that can then
 * be used for matching.
 * @param <S> the type of the state object whose actual type depends on the implementation of the concrete visitor
 */
@SuppressWarnings("java:S3252")
public interface ExpansionVisitor<S extends KeyExpressionVisitor.State> extends KeyExpressionVisitor<S, GraphExpansion> {
    /**
     * Method that expands a data structure into a data flow graph
     * @param baseQuantifier a quantifier representing the base data access
     * @param primaryKey the primary key of the data object the caller wants to access
     * @param isReverse an indicator whether the result set is expected to be returned in reverse order
     * @return a new {@link MatchCandidate} that can be used for matching.
     */
    @Nonnull
    MatchCandidate expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                          @Nullable final KeyExpression primaryKey,
                          final boolean isReverse);
}
