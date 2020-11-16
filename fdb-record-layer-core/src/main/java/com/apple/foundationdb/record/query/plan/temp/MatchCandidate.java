/*
 * MatchCandidate.java
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

package com.apple.foundationdb.record.query.plan.temp;

import javax.annotation.Nonnull;

/**
 * Case class to represent a match candidate. A match candidate on code level is just a name and a data flow graph
 * that can be matched against a query graph. The match candidate does not keep the root to the graph to be matched but
 * rather an instance of {@link ExpressionRefTraversal} to allow for navigation of references within the candidate.
 */
public class MatchCandidate {
    /**
     * Name of the match candidate. If this candidate represents and index, it will be the name of the index.
     */
    @Nonnull
    private final String name;

    /**
     * Traversal object.
     */
    @Nonnull
    private final ExpressionRefTraversal traversal;

    public MatchCandidate(@Nonnull String name, @Nonnull final ExpressionRefTraversal traversal) {
        this.name = name;
        this.traversal = traversal;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public ExpressionRefTraversal getTraversal() {
        return traversal;
    }
}
