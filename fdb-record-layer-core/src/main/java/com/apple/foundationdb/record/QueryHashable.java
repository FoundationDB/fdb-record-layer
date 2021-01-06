/*
 * QueryHashable.java
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

package com.apple.foundationdb.record;

import javax.annotation.Nonnull;

/**
 * Query hash - calculate and return identifying hash values for queries.
 * The queryHash semantics are different than {@link Object#hashCode} in a few ways:
 * <UL>
 *     <LI>{@link #queryHash} values should be stable across runtime instance changes. The reason is that these values are going to be used
 *     to correlate queries with plans across multiple instances, and so need to be repeatable and stable.</LI>
 *     <LI>{@link #queryHash} supports multiple flavors of hash calculations (See {@link QueryHashKind}). The various kinds of hash values
 *     are used for different purposes and include/exclude different parts of the target query</LI>
 *     <LI>{@link #queryHash} is meant to imply a certain identity of a query, and reflects on the entire structure of the query.
 *     The intent is to be able to correlate various queries for "identity" (using different definitions for this identity as
 *     specified by {@link QueryHashKind}). This requirement drives a desire to reduce collisions as much as possible since
 *     not in all cases can we actually use "equals" to verify identity (e.g. log messages)</LI>
 * </UL>
 * See also {@link PlanHashable}
 */
public interface QueryHashable {
    /**
     * The "kinds" of queryHash calculations.
     */
    enum QueryHashKind {
        STRUCTURAL_WITHOUT_LITERALS   // The hash used for query and plan matching: skip all literals and markers
    }

    /**
     * Return a hash similar to <code>hashCode</code>, but with the additional guarantee that is is stable across JVMs.
     * @param hashKind the "kind" of hash to calculate. Each kind of hash has a particular logic with regards to included and excluded items.
     * @return a stable hash code
     */
    int queryHash(@Nonnull final QueryHashKind hashKind);
}
