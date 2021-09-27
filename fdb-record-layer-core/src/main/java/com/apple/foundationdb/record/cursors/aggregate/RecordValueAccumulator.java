/*
 * SimpleAccumulator.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An {@link AggregateAccumulator} for a single {@link Value}. This accumulator can evaluate a record (e.g. {@link
 * com.apple.foundationdb.record.query.predicates.FieldValue}
 * and accumulate the results.
 *
 * @param <T> the type of value the state accumulates holds
 * @param <R> the type of the result that gets returned once the aggregator's {@link #finish} is called.
 */
public class RecordValueAccumulator<T, R> {
    @Nonnull
    private Value value;
    @Nonnull
    private AccumulatorState<T, R> state;

    public RecordValueAccumulator(@Nonnull final Value value, AccumulatorState<T, R> state) {
        this.value = value;
        this.state = state;
    }

    public <M extends Message> void accumulate(@Nonnull FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                               @Nullable final FDBRecord<M> record, @Nonnull final M message) {
        state.accumulate(narrow(value.eval(store, context, record, message)));
    }

    @Nullable
    public R finish() {
        return state.finish();
    }

    @SuppressWarnings("unchecked")
    private T narrow(final Object rawValue) {
        // This should not fail since the planner should be feeding the correct types to the states and values.
        return (T)rawValue;
    }
}
