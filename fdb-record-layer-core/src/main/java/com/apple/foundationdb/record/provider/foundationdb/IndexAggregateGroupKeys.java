/*
 * IndexAggregateGroupKeys.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Helper class for extracting grouping keys from an {@link IndexAggregateFunction} given query conditions.
 *
 * To implement an aggregate function <code>XXX(value BY group)</code> using an index, the group needs to be fixed,
 * which will be true when the query has a {@link com.apple.foundationdb.record.query.RecordQuery#getFilter filter} on <code>group</code> (equivalent to SQL <code>WHERE group = ?</code>).
 */
@API(API.Status.EXPERIMENTAL)
public abstract class IndexAggregateGroupKeys {
    /**
     * Get the grouping key (GROUP BY) for the index aggregate from the given context. 
     * @param store the record store for the query
     * @param context context in which to evaluate keys
     * @return the grouping key
     */
    @Nonnull
    public abstract Key.Evaluated getGroupKeys(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context);

    /**
     * Get the size of the prefix of the index aggregate that is returned.
     * That is, the size of {#link #getGroupKeys}.
     * @return number of items in the grouping key
     */
    public abstract int getColumnSize();

    public static Optional<IndexAggregateGroupKeys> conditionsToGroupKeys(@Nonnull IndexAggregateFunction function,
                                                                          @Nullable QueryComponent conditions) {
        return conditionsToGroupKeys(function.getOperand(), conditions);
    }

    @SuppressWarnings("PMD.EmptyCatchBlock")
    public static Optional<IndexAggregateGroupKeys> conditionsToGroupKeys(@Nonnull KeyExpression operand,
                                                                          @Nullable QueryComponent conditions) {
        final KeyExpression groupingKey = IndexFunctionHelper.getGroupingKey(operand);
        if (conditions == null) {
            if (groupingKey.getColumnSize() == 0) {
                return Optional.of(new Conditions(Collections.emptyList()));
            }
        } else {
            final QueryToKeyMatcher matcher = new QueryToKeyMatcher(conditions);
            final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(groupingKey);
            if (match.getType() != QueryToKeyMatcher.MatchType.NO_MATCH) {
                return Optional.of(new Conditions(match.getEqualityComparisons()));
            }
        }
        return Optional.empty();
    }

    public static IndexAggregateGroupKeys indexScanToGroupKeys(@Nonnull String recordKey, int prefixSize) {
        return new IndexScan(recordKey, prefixSize);
    }

    @API(API.Status.EXPERIMENTAL)
    protected static class Conditions extends IndexAggregateGroupKeys {
        @Nonnull
        private final List<Comparisons.Comparison> comparisons;

        protected Conditions(@Nonnull List<Comparisons.Comparison> comparisons) {
            this.comparisons = comparisons;
        }

        @Override
        @Nonnull
        public Key.Evaluated getGroupKeys(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (comparisons.isEmpty()) {
                return Key.Evaluated.EMPTY;
            } else {
                return Key.Evaluated.concatenate(comparisons.stream().map(c -> c.getComparand(store, context)).collect(Collectors.toList()));
            }
        }

        @Override
        public int getColumnSize() {
            return comparisons.size();
        }
    }

    @API(API.Status.EXPERIMENTAL)
    protected static class IndexScan extends IndexAggregateGroupKeys {
        @Nonnull
        private final String recordKey;
        private final int prefixSize;

        protected IndexScan(@Nonnull String recordKey, int prefixSize) {
            this.recordKey = recordKey;
            this.prefixSize = prefixSize;
        }

        @Override
        @Nonnull
        public Key.Evaluated getGroupKeys(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (context == null) {
                throw new Comparisons.EvaluationContextRequiredException("Cannot get parameter without context");
            }
            final FDBQueriedRecord<?> record = (FDBQueriedRecord<?>) context.getBinding(recordKey);
            final IndexEntry indexEntry = Objects.requireNonNull(record.getIndexEntry());
            return Key.Evaluated.fromTuple(TupleHelpers.subTuple(indexEntry.getKey(), 0, prefixSize));
        }

        @Override
        public int getColumnSize() {
            return prefixSize;
        }
    }
    
}
