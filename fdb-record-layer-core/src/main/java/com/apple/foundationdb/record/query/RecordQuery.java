/*
 * RecordQuery.java
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

package com.apple.foundationdb.record.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.QueryHashable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The logical form of a query.
 *
 * A query mainly consists of:
 * <ul>
 * <li>The names of the record type(s) to query.</li>
 * <li>A {@link QueryComponent} filter.</li>
 * <li>A sort key.</li>
 * </ul>
 * Executing a query means returning records of the given type(s) that match the filter in the indicated order.
 *
 * @see com.apple.foundationdb.record.query.plan.RecordQueryPlanner#plan
 */
@API(API.Status.STABLE)
public class RecordQuery implements QueryHashable {
    public static final Collection<String> ALL_TYPES = Collections.emptyList();
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query");

    @Nonnull
    private final Collection<String> recordTypes;
    @Nullable
    private final Collection<String> allowedIndexes;
    @Nonnull
    private final IndexQueryabilityFilter queryabilityFilter;
    @Nullable
    private final QueryComponent filter;
    @Nullable
    private final KeyExpression sort;
    private final boolean sortReverse;
    private final boolean removeDuplicates;
    @Nullable
    private final List<KeyExpression> requiredResults;

    private RecordQuery(@Nonnull Collection<String> recordTypes,
                        @Nullable Collection<String> allowedIndexes,
                        @Nonnull IndexQueryabilityFilter queryabilityFilter,
                        @Nullable QueryComponent filter,
                        @Nullable KeyExpression sort,
                        boolean sortReverse,
                        boolean removeDuplicates,
                        @Nullable List<KeyExpression> requiredResults) {
        this.recordTypes = recordTypes;
        this.allowedIndexes = allowedIndexes;
        this.queryabilityFilter = queryabilityFilter;
        this.filter = filter;
        this.sort = sort;
        this.sortReverse = sortReverse;
        this.removeDuplicates = removeDuplicates;
        this.requiredResults = requiredResults;
    }

    @Nonnull
    public Collection<String> getRecordTypes() {
        return recordTypes;
    }

    public boolean hasAllowedIndexes() {
        return allowedIndexes != null;
    }

    @Nullable
    public Collection<String> getAllowedIndexes() {
        return allowedIndexes;
    }

    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public IndexQueryabilityFilter getIndexQueryabilityFilter() {
        return queryabilityFilter;
    }

    @Nullable
    public QueryComponent getFilter() {
        return filter;
    }

    @Nullable
    public KeyExpression getSort() {
        return sort;
    }

    public boolean isSortReverse() {
        return sortReverse;
    }

    public boolean removesDuplicates() {
        return removeDuplicates;
    }

    @Nullable
    public List<KeyExpression> getRequiredResults() {
        return requiredResults;
    }

    /**
     * Validates that this record query is valid with the provided metadata.
     * @param metaData the metadata that you want to use with this query
     */
    public void validate(@Nonnull RecordMetaData metaData) {
        for (String recordTypeName : recordTypes) {
            final RecordType recordType = metaData.getRecordType(recordTypeName);
            final Descriptors.Descriptor descriptor = recordType.getDescriptor();
            if (filter != null) {
                filter.validate(descriptor);
            }
            if (sort != null) {
                sort.validate(descriptor);
            }
            if (requiredResults != null) {
                for (KeyExpression result : requiredResults) {
                    result.validate(descriptor);
                }
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder str = new StringBuilder();
        str.append(recordTypes);
        if (filter != null) {
            str.append(" | ").append(filter);
        }
        return str.toString();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    @Override
    public int queryHash(@Nonnull final QueryHashable.QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, recordTypes, allowedIndexes, queryabilityFilter, filter, sort, sortReverse, removeDuplicates);
    }

    /**
     * A builder for {@link RecordQuery}.
     *
     * <pre><code>
     * RecordQuery.newBuilder()
     *                 .setRecordType("MyRecord")
     *                 .setFilter(Query.and(
     *                         Query.field("str_value").equalsValue("xyz"),
     *                         Query.field("num_value").equalsValue(123)))
     *                 .setSort(field("rec_no"))
     *                 .build()
     * </code></pre>
     */
    public static class Builder {
        @Nonnull
        private Collection<String> recordTypes = ALL_TYPES;
        @Nullable
        private Collection<String> allowedIndexes = null;
        @Nonnull
        private IndexQueryabilityFilter queryabilityFilter = IndexQueryabilityFilter.DEFAULT;
        @Nullable
        private QueryComponent filter = null;
        @Nullable
        private KeyExpression sort = null;
        private boolean sortReverse;
        private boolean removeDuplicates = true;
        @Nullable
        private List<KeyExpression> requiredResults = null;

        protected Builder() {
        }

        protected Builder(RecordQuery query) {
            this.recordTypes = query.recordTypes;
            this.allowedIndexes = query.allowedIndexes;
            this.queryabilityFilter = query.queryabilityFilter;
            this.filter = query.filter;
            this.sort = query.sort;
            this.sortReverse = query.sortReverse;
            this.removeDuplicates = query.removeDuplicates;
            this.requiredResults = query.requiredResults;
        }

        public RecordQuery build() {
            return new RecordQuery(recordTypes, allowedIndexes, queryabilityFilter,
                    filter, sort, sortReverse, removeDuplicates, requiredResults);
        }

        @Nonnull
        public Collection<String> getRecordTypes() {
            return recordTypes;
        }

        public Builder setRecordTypes(@Nonnull Collection<String> recordTypes) {
            this.recordTypes = recordTypes;
            return this;
        }

        public Builder setRecordType(@Nonnull String recordType) {
            return setRecordTypes(Collections.singleton(recordType));
        }

        @Nullable
        public Collection<String> getAllowedIndexes() {
            return allowedIndexes;
        }

        /**
         * Define the indexes that the planner can consider when planning the query.
         * If set, the allowed indexes override the index queryability filter.
         * @param allowedIndexes a collection of index names
         * @return this builder
         */
        public Builder setAllowedIndexes(@Nullable Collection<String> allowedIndexes) {
            this.allowedIndexes = allowedIndexes;
            return this;
        }

        public Builder setAllowedIndex(@Nullable String allowedIndex) {
            return setAllowedIndexes(Collections.singleton(allowedIndex));
        }

        /**
         * Set a function that defines whether each index should be used by the query planner.
         * Note that if allowed indexes are used then the queryability filter is ignored.
         * The default index queryability filter is {@link IndexQueryabilityFilter#DEFAULT}.
         * @param queryabilityFilter a queryability filter to use
         * @return this builder
         */
        public Builder setIndexQueryabilityFilter(@Nonnull IndexQueryabilityFilter queryabilityFilter) {
            this.queryabilityFilter = queryabilityFilter;
            return this;
        }

        @Nullable
        public QueryComponent getFilter() {
            return filter;
        }

        public Builder setFilter(@Nullable QueryComponent filter) {
            this.filter = filter;
            return this;
        }

        @Nullable
        public KeyExpression getSort() {
            return sort;
        }

        public boolean isSortReverse() {
            return sortReverse;
        }

        public Builder setSort(@Nullable KeyExpression sort, boolean sortReverse) {
            this.sort = sort;
            this.sortReverse = sortReverse;
            return this;
        }

        public Builder setSort(@Nullable KeyExpression sort) {
            return setSort(sort, false);
        }

        public Builder setRemoveDuplicates(boolean removeDuplicates) {
            this.removeDuplicates = removeDuplicates;
            return this;
        }

        @Nullable
        public List<KeyExpression> getRequiredResults() {
            return requiredResults;
        }

        public Builder setRequiredResults(@Nullable List<KeyExpression> requiredResults) {
            this.requiredResults = requiredResults;
            return this;
        }
    }
}
