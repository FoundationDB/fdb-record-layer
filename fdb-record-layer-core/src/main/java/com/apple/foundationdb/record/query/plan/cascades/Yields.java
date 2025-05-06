/*
 * Memoizer.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;

/**
 * Interface extending both {@link ExploratoryYields} and {@link FinalYields} and adding yielding capabilities for
 * {@link PartialMatch}es.
 */
@API(API.Status.EXPERIMENTAL)
public interface Yields extends ExploratoryYields, FinalYields {
    /**
     * Notify the planner's data structures that a new partial match has been produced by the rule. This method may be
     * called zero or more times by the rule's <code>onMatch()</code> method.
     *
     * @param boundAliasMap the alias map of bound correlated identifiers between query and candidate
     * @param matchCandidate the match candidate
     * @param queryExpression the query expression
     * @param candidateRef the matching reference on the candidate side
     * @param matchInfo an auxiliary structure to keep additional information about the match
     */
    void yieldPartialMatch(@Nonnull AliasMap boundAliasMap,
                           @Nonnull MatchCandidate matchCandidate,
                           @Nonnull RelationalExpression queryExpression,
                           @Nonnull Reference candidateRef,
                           @Nonnull MatchInfo matchInfo);
}
