/*
 * IndexQueryabilityFilter.java
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

package com.apple.foundationdb.record.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.QueryHashable;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;

import javax.annotation.Nonnull;

/**
 * A filter used to determine whether an index should be considered when planning queries.
 */
@API(API.Status.EXPERIMENTAL)
@FunctionalInterface
public interface IndexQueryabilityFilter extends QueryHashable {
    /**
     * The default index queryability filter which uses all indexes except those with the
     * {@link IndexOptions#ALLOWED_FOR_QUERY_OPTION} set to {@code false}.
     */
    IndexQueryabilityFilter DEFAULT = new IndexQueryabilityFilter() {
        @Override
        public boolean isQueryable(@Nonnull final Index index) {
            return index.getBooleanOption(IndexOptions.ALLOWED_FOR_QUERY_OPTION, true);
        }

        @Override
        public int queryHash(@Nonnull final QueryHashable.QueryHashKind hashKind) {
            return "DEFAULT_INDEX_QUERYABILITY_FILTER".hashCode();
        }
    };

    /**
     * Return whether the given index should be considered by the query planner. Note that the planner is not required
     * to use an index for which {@code isQueryable()} is {@code true}.
     * @param index an index
     * @return whether the given index should be considered by the planner
     */
    boolean isQueryable(@Nonnull Index index);

    /**
     * Default implementation of {@link QueryHashable#queryHash}. This implementation returns '0' so unless the specific concrete implementation of
     * {@link IndexQueryabilityFilter} overrides it, there will be no impact on the total hashcode for the query.
     * <p>Note:
     * This should be overridden in specific implementations in order to be able to distinguish among various queries that
     * differ only by their {@link IndexQueryabilityFilter}.
     * @param hashKind the "kind" of hash to calculate. Ignored for this implementation.
     * @return 0 as the calculated hash in all cases, to have no impact on query hashes.
     */
    @Override
    default int queryHash(@Nonnull final QueryHashable.QueryHashKind hashKind) {
        return 0;
    }
}
