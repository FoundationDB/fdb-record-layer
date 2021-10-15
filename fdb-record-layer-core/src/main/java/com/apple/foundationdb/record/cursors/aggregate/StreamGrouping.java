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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * StreamGrouping breaks streams of records into groups, based on grouping criteria.
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
public class StreamGrouping<M extends Message> {
    @Nonnull
    private final FDBRecordStoreBase<M> store;
    @Nonnull
    private final EvaluationContext context;
    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final List<Value> groupingKeys;
    @Nonnull
    private final List<AggregateValue<?, ?>> aggregateValues;
    @Nonnull
    private AccumulatorList accumulator;
    // The current group (evaluated). This will be used to decide if the next record is a group break
    @Nullable
    private List<Object> currentGroup;
    // The previous completed group result - with both grouping criteria and accumulated values
    @Nullable
    private List<Object> previousGroupResult;

    /**
     * Create a new group aggregator.
     *
     * @param groupingKeys the list of grouping criteria. The list is ordered, so that the first criteria is the
     * "coarse" group and the last criteria is the "fine" group.
     * @param aggregateValues the list of aggregate values that will accumulate the fields. The list is ordered such
     * that the result will have the aggregates in the same order as specified by the list
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param alias the quantifier alias for the value evaluation
     */
    public StreamGrouping(@Nonnull final List<Value> groupingKeys, @Nonnull List<AggregateValue<?, ?>> aggregateValues,
                          @Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nonnull CorrelationIdentifier alias) {
        this.groupingKeys = groupingKeys;
        this.aggregateValues = aggregateValues;
        this.accumulator = createAccumulator(aggregateValues);
        this.store = store;
        this.context = context;
        this.alias = alias;
    }

    /**
     * Initialize the state for the class by applying the given state to the accumulators and grouping keys. The given values
     * are restored from continuations and are to set the state to the state that was stored in a continuation previously.
     * @param groupingStates the Grouping Keys state to use
     * @param accumulatorStates the accumulator values to use
     */
    public void setContinuationState(@Nullable List<AggregateCursorContinuation.ContinuationGroupingKeyState> groupingStates, @Nullable List<AggregateCursorContinuation.ContinuationAccumulatorState> accumulatorStates) {
        if ((groupingStates != null) && !groupingStates.isEmpty()) {
            if (groupingStates.size() != groupingKeys.size()) {
                throw new RecordCoreException("Failed to initialize from continuation: size of grouping keys does not match")
                        .addLogInfo("size", groupingKeys.size())
                        .addLogInfo("givenSize", groupingStates.size());
            }
            currentGroup = groupingStates.stream().map(AggregateCursorContinuation.ContinuationGroupingKeyState::getStateValue).collect(Collectors.toList());
        }
        if ((accumulatorStates != null) && !accumulatorStates.isEmpty()) {
            if (accumulatorStates.size() != aggregateValues.size()) {
                throw new RecordCoreException("Failed to initialize from continuation: size of aggregator values does not match")
                        .addLogInfo("size", aggregateValues.size())
                        .addLogInfo("givenSize", accumulatorStates.size());
            }
            accumulator.setContinuationState(accumulatorStates);
        }
    }

    @Nonnull
    public List<AggregateCursorContinuation.ContinuationGroupingKeyState> getGroupingKeyStates() {
        return (currentGroup != null) ? currentGroup.stream().map(this::toGroupingKeyState).collect(Collectors.toList()) : Collections.emptyList();
    }

    @Nonnull
    public List<AggregateCursorContinuation.ContinuationAccumulatorState> getAccumulatorStates() {
        return accumulator.getContinuationState();
    }

    /**
     * Accept the next record, decide whether this next record constitutes a group break.
     * Following these rules:
     * <UL>
     * <LI>The last item (record that has {@link RecordCursorResult#hasNext()} return false), is <b>always</b> a groupbreak</LI>
     * <LI>The first record is <b>never</b> a group break, unless it is last</LI>
     * <LI>When of a record's grouping criteria changes from the previous record, this is a group break</LI>
     * </UL>
     *
     * @param record the next record to process
     *
     * @return true IFF the next record provided constitutes a group break
     */
    public boolean apply(@Nonnull FDBQueriedRecord<M> record) {
        List<Object> nextGroup = eval(record, groupingKeys);
        boolean groupBreak = isGroupBreak(currentGroup, nextGroup);
        if (groupBreak) {
            finalizeGroupInternal(nextGroup);
        } else {
            // for the case where we have no current group (first group encountered)
            currentGroup = nextGroup;
        }
        accumulate(record);
        return groupBreak;
    }

    /**
     * Get the last completed group result. This will return the group that was completed last (prior to the last group
     * break).
     *
     * @return the last result aggregated.
     */
    @Nonnull
    public List<Object> getCompletedGroupResult() {
        if (previousGroupResult == null) {
            throw new RecordCoreException("Attempt to get a completedGroupResult without a group ever being finalized");
        }
        return previousGroupResult;
    }

    /**
     * An indication that the streaming is done and the last group needs to be finalized.
     */
    public void finalizeGroup() {
        finalizeGroupInternal(null);
    }

    /**
     * Whether theer are grouing criteria (keys) in this streamGrouping.
     * @return TRUE if there are any grouping keys in this streamGrouping, FALSE otherwise.
     */
    public boolean hasGroupingCriteria() {
        return !groupingKeys.isEmpty();
    }


    private boolean isGroupBreak(final List<Object> currentGroup, final List<Object> nextGroup) {
        if ((currentGroup == null) || (currentGroup.isEmpty())) {
            return false;
        } else {
            return (!currentGroup.equals(nextGroup));
        }
    }

    private void finalizeGroupInternal(List<Object> nextGroup) {
        // Cannot use ImmutableList since it does not support null elements.
        List<Object> result = new ArrayList<>();
        if (currentGroup != null) {
            result.addAll(currentGroup);
        }
        result.addAll(accumulator.finish());
        previousGroupResult = Collections.unmodifiableList(result);

        currentGroup = nextGroup;
        // "Reset" the accumulator by creating a fresh one.
        accumulator = createAccumulator(aggregateValues);
    }

    private void accumulate(final @Nonnull FDBQueriedRecord<M> record) {
        EvaluationContext nestedContext = context.withBinding(alias, record.getRecord());
        accumulator.accumulate(store, nestedContext, record, record.getRecord());
    }

    private List<Object> eval(final FDBQueriedRecord<M> record, List<Value> values) {
        final EvaluationContext nestedContext = context.withBinding(alias, record.getRecord());
        return values.stream()
                .map(value -> value.eval(store, nestedContext, record, record.getRecord()))
                .collect(Collectors.toList());
    }

    @Nonnull
    private AccumulatorList createAccumulator(final @Nonnull List<AggregateValue<?, ?>> aggregateValues) {
        return new AccumulatorList(aggregateValues.stream().map(AggregateValue::createAccumulator).collect(ImmutableList.toImmutableList()));
    }

    private AggregateCursorContinuation.ContinuationGroupingKeyState toGroupingKeyState(final @Nonnull Object o) {
        if (o instanceof Integer) {
            return new AggregateCursorContinuation.ContinuationGroupingKeyState((Integer)o);
        } else if (o instanceof Long) {
            return new AggregateCursorContinuation.ContinuationGroupingKeyState((Long)o);
        } else if (o instanceof Float) {
            return new AggregateCursorContinuation.ContinuationGroupingKeyState((Float)o);
        } else if (o instanceof Double) {
            return new AggregateCursorContinuation.ContinuationGroupingKeyState((Double)o);
        } else if (o instanceof String) {
            return new AggregateCursorContinuation.ContinuationGroupingKeyState((String)o);
        } else {
            throw new RecordCoreException("Cannot create GroupingKeyState: Unsupported type")
                    .addLogInfo("type", o.getClass().getSimpleName());
        }
    }
}
