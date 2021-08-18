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
import com.apple.foundationdb.record.query.plan.plans.QueryResultElement;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * An {@link AggregateAccumulator} for a single {@link com.apple.foundationdb.record.query.predicates.AggregateValue AggregateValue}.
 * @param <S> the type of state carried through the accumulation
 * @param <T> the type of result that is being aggregated (e.g. Integer for SUM).
 */
public class SimpleAccumulator<S, T> implements AggregateAccumulator {
    @Nonnull
    private AggregateValue<S> value;
    @Nonnull
    private S currentState;

    public SimpleAccumulator(@Nonnull final AggregateValue<S> value) {
        this.value = value;
        currentState = value.initial();
    }

    @Override
    public void reset() {
        currentState = value.initial();
    }

    @Override
    public <M extends Message> void accumulate(@Nonnull FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                               @Nullable final FDBRecord<M> record, @Nonnull final M message) {
        currentState = value.accumulate(currentState, store, context, record, message);
    }

    @Override
    public List<QueryResultElement> finish() {
        return value.finish(currentState);
    }
}
