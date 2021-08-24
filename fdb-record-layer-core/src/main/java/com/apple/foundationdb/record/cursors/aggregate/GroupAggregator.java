/*
 * GroupAggregator.java
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

package com.apple.foundationdb.record.cursors.aggregate;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.plans.QueryResultElement;
import com.apple.foundationdb.record.query.plan.plans.SingularResultElement;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GroupAggregator breaks streams of records into groups, based on grouping criteria.
 * For each record (provided through {@link #apply}) the class would decide whether this record is part
 * of the current group or of the next group.
 * <p>Note: If there are no grouping criteria provided, (the 'zero case'), then all records are placed in one group.</p>
 * For example, given the grouping criteria <pre>{'a', 'b'}</pre> and the records
 * <pre>
 * (1) {'a' : 1; 'b' : 1; 'c' : 1}
 * (2) {'a' : 1; 'b' : 1; 'c' : 2}
 * (3) {'a' : 1; 'b' : 1; 'c' : 2}
 * (4) {'a' : 1; 'b' : 2; 'c' : 1}
 * (5) {'a' : 1; 'b' : 2; 'c' : 2}
 * </pre>
 * We would get 2 groups:
 * <pre>
 * {'a' : 1; 'b' : 1}
 * {'a' : 1; 'b' : 2}
 * </pre>
 * <p>
 * The records are assumed to be compatibly ordered with the grouping criteria. That is, if the grouping criteria are
 * {'a', 'b'}
 * then the records are expected to be sorted by at least {'a', 'b'} (though {'a', 'b', 'c'} is also compatible).
 *
 * @param <M> the type of content (message) that is in the store
 */
public class GroupAggregator<M extends Message> {
    @Nonnull
    private final FDBRecordStoreBase<M> store;
    @Nonnull
    private final EvaluationContext context;
    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final List<Value> groupCriteria;
    @Nonnull
    private List<AggregateValue<?>> aggregateValues;
    @Nonnull
    private AggregateAccumulator accumulator;
    // The current group (evaluated). This will be used to decide if the next record is a group break
    @Nullable
    private List<QueryResultElement> currentGroup;
    // The previous completed group result - with both grouping criteria and accumulated values
    @Nullable
    private List<QueryResultElement> previousGroupResult;

    /**
     * Create a new group aggregator.
     *
     * @param groupCriteria the list of grouping criteria. The list is ordered, so that the first criteria is the
     * "coarse" group and the last criteria is the "fine" group.
     * @param aggregateValues the list of aggregate values that will accumulate the fields. The list is ordered such
     * that the result will have the aggregates in the same order as specified by the list
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param alias the quantifier alias for the value evaluation
     */
    public GroupAggregator(@Nonnull final List<Value> groupCriteria, @Nonnull List<AggregateValue<?>> aggregateValues,
                           @Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nonnull CorrelationIdentifier alias) {
        this.groupCriteria = groupCriteria;
        this.aggregateValues = aggregateValues;
        this.accumulator = createAccumulator(aggregateValues);
        this.store = store;
        this.context = context;
        this.alias = alias;
    }

    /**
     * Accept the next record, decide whether this next record constitutes a group break.
     * Following these rules:
     * <UL>
     * <LI>The last item (record that has {@link RecordCursorResult#hasNext()} return false), is <b>always</b> a group
     * break</LI>
     * <LI>The first record is <b>never</b> a group break, unless it is last</LI>
     * <LI>When of a record's grouping criteria changes from the previous record, this is a group break</LI>
     * </UL>
     *
     * @param record the next record to process
     *
     * @return true IFF the next record provided constitutes a group break
     */
    public boolean apply(@Nonnull FDBQueriedRecord<M> record) {
        List<QueryResultElement> nextGroup = eval(record, groupCriteria);
        boolean groupBreak = isGroupBreak(currentGroup, nextGroup);
        if (groupBreak) {
            finalizeGroup(nextGroup);
        } else {
            // for the case where we have no current group. In most cases, this changes nothing
            currentGroup = nextGroup;
        }
        accumulate(record);
        return groupBreak;
    }

    /**
     * Get the last completed group result. This will return the group that was completed last (prior to the last group
     * break).
     *
     * @return the last result aggregated. Null if no group was completed by this aggregator.
     */
    @Nullable
    public List<QueryResultElement> getCompletedGroupResult() {
        return previousGroupResult;
    }

    private boolean isGroupBreak(final List<QueryResultElement> currentGroup, final List<QueryResultElement> nextGroup) {
        if ((currentGroup == null) || (currentGroup.isEmpty())) {
            return false;
        } else {
            return (!currentGroup.equals(nextGroup));
        }
    }

    public void finalizeGroup() {
        finalizeGroup(null);
    }

    private void finalizeGroup(List<QueryResultElement> nextGroup) {
        previousGroupResult = ImmutableList.<QueryResultElement>builder()
                .addAll(currentGroup != null ? currentGroup : Collections.emptyList())
                .addAll(accumulator.finish())
                .build();
        currentGroup = nextGroup;
        // "Reset" the accumulator by creating a fresh one.
        accumulator = createAccumulator(aggregateValues);
    }

    @SuppressWarnings("unchecked")
    private void accumulate(final @Nonnull FDBQueriedRecord<M> record) {
        EvaluationContext nestedContext = context.withBinding(alias, record.getRecord());
        accumulator.accumulate(store, nestedContext, record, record.getRecord());
    }

    @SuppressWarnings("unchecked")
    private List<QueryResultElement> eval(final FDBQueriedRecord<M> record, List<Value> values) {
        final EvaluationContext nestedContext = context.withBinding(alias, record.getRecord());
        return values.stream()
                .map(value -> value.eval(store, nestedContext, record, record.getRecord()))
                .map(this::narrow)
                .collect(Collectors.toList());
    }

    @Nonnull
    private AggregateAccumulator createAccumulator(final @Nonnull List<AggregateValue<?>> aggregateValues) {
        return new AccumulatorList(aggregateValues.stream().map(SimpleAccumulator::new).collect(Collectors.toList()));
    }

    // TODO: This needs to be moved into the Value.eval() method (that currently returns Object).
    private QueryResultElement narrow(Object rawValue) {
        if (rawValue instanceof Integer) {
            return SingularResultElement.of((Integer)rawValue);
        } else if (rawValue instanceof Long) {
            return SingularResultElement.of((Long)rawValue);
        } else if (rawValue instanceof Float) {
            return SingularResultElement.of((Float)rawValue);
        } else if (rawValue instanceof Double) {
            return SingularResultElement.of((Double)rawValue);
        } else if (rawValue instanceof String) {
            return SingularResultElement.of((String)rawValue);
        } else {
            // TODO: Get correct exception
            throw new RuntimeException("Unrecognized type in aggregation: " + rawValue.getClass().getSimpleName());
        }
    }
}
