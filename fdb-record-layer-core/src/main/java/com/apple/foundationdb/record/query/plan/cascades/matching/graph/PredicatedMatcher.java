/*
 * PredicatedMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.graph;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;

/**
 * Tag interface for predicate matchers, that is instances of {@link FindingMatcher}. This interface abstracts away
 * the element type.
 */

public interface PredicatedMatcher {
    /**
     * Find complete matches.
     * @return an {@link Iterable} of {@link AliasMap}s where each {@link AliasMap} is considered a match.
     */
    Iterable<AliasMap> findCompleteMatches();

    /**
     * Find matches.
     * @return an {@link Iterable} of {@link AliasMap}s where each {@link AliasMap} is considered a match.
     */
    Iterable<AliasMap> findMatches();
}
