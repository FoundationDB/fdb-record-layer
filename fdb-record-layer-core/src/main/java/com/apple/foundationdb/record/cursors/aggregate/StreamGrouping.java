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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.predicates.Accumulator;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

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
    @Nullable
    private final Value groupingKeyValue;
    @Nonnull
    private final AggregateValue aggregateValue;
    @Nonnull
    private final FDBRecordStoreBase<M> store;
    @Nonnull
    private final EvaluationContext context;
    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private Accumulator accumulator;
    // The current group (evaluated). This will be used to decide if the next record is a group break
    @Nullable
    private Object currentGroup;
    // The previous completed group result - with both grouping criteria and accumulated values
    @Nullable
    private Object previousCompleteResult;
    @Nonnull
    private final CorrelationIdentifier groupingKeyAlias;
    @Nonnull
    private final CorrelationIdentifier aggregateAlias;
    @Nonnull
    private final Value completeResultValue;

    /**
     * Create a new group aggregator.
     *
     * @param groupingKeyValue the grouping key
     * @param aggregateValue the aggregate values that will accumulate the fields
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param alias the quantifier alias for the value evaluation
     */
    public StreamGrouping(@Nullable final Value groupingKeyValue,
                          @Nonnull final AggregateValue aggregateValue,
                          @Nonnull final Value completeResultValue,
                          @Nonnull final CorrelationIdentifier groupingKeyAlias,
                          @Nonnull final CorrelationIdentifier aggregateAlias,
                          @Nonnull final FDBRecordStoreBase<M> store,
                          @Nonnull final EvaluationContext context,
                          @Nonnull final CorrelationIdentifier alias) {
        this.groupingKeyValue = groupingKeyValue;
        this.aggregateValue = aggregateValue;
        this.accumulator = aggregateValue.createAccumulator(context.getTypeRepository());
        this.store = store;
        this.context = context;
        this.alias = alias;
        this.groupingKeyAlias = groupingKeyAlias;
        this.aggregateAlias = aggregateAlias;
        this.completeResultValue = completeResultValue;
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
     * @param currentObject the next object to process
     *
     * @return true if and only if the next object provided constitutes a group break
     */
    public boolean apply(@Nullable Object currentObject) {
        final boolean groupBreak;
        if (groupingKeyValue != null) {
            Object nextGroup = evalGroupingKey(currentObject);
            groupBreak = isGroupBreak(currentGroup, nextGroup);
            if (groupBreak) {
                finalizeGroup(nextGroup);
            } else {
                // for the case where we have no current group. In most cases, this changes nothing
                currentGroup = nextGroup;
            }
        } else {
            groupBreak = false;
            currentGroup = null;
        }
        accumulate(currentObject);
        return groupBreak;
    }

    /**
     * Get the last completed group result. This will return the group that was completed last (prior to the last group
     * break).
     *
     * @return the last result aggregated. Null if no group was completed by this aggregator.
     */
    @Nullable
    public Object getCompletedGroupResult() {
        return previousCompleteResult;
    }

    private boolean isGroupBreak(final Object currentGroup, final Object nextGroup) {
        if (currentGroup == null) {
            return false;
        } else {
            return (!currentGroup.equals(nextGroup));
        }
    }

    public void finalizeGroup() {
        finalizeGroup(null);
    }

    private void finalizeGroup(Object nextGroup) {
        final EvaluationContext nestedContext = context.childBuilder()
                .setBinding(groupingKeyAlias, currentGroup)
                .setBinding(aggregateAlias, accumulator.finish())
                .build(context.getTypeRepository());
        previousCompleteResult = completeResultValue.eval(store, nestedContext, null, null);

        currentGroup = nextGroup;
        // "Reset" the accumulator by creating a fresh one.
        accumulator = aggregateValue.createAccumulator(context.getTypeRepository());
    }

    private void accumulate(@Nullable Object currentObject) {
        EvaluationContext nestedContext = context.withBinding(alias, currentObject);
        final Object partial = aggregateValue.evalToPartial(store, nestedContext, null, null);
        accumulator.accumulate(partial);
    }

    private Object evalGroupingKey(@Nullable final Object currentObject) {
        final EvaluationContext nestedContext = context.withBinding(alias, currentObject);
        return Objects.requireNonNull(groupingKeyValue).eval(store, nestedContext, null, null);
    }

    public boolean isResultOnEmpty() {
        return groupingKeyValue == null;
    }
}
