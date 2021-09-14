/*
 * AccumulatorList.java
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
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A list of {@link AggregateAccumulator}s.
 * This class applies aggregation operations to all of its accumulators and return the combined result.
 */
public class AccumulatorList {
    @Nonnull
    private final List<RecordValueAccumulator<?, ?>> accumulators;

    public AccumulatorList(@Nonnull final List<RecordValueAccumulator<?, ?>> accumulators) {
        this.accumulators = new ArrayList<>(accumulators);
    }

    public <M extends Message> void accumulate(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                               @Nullable final FDBRecord<M> record, @Nonnull final M message) {
        accumulators.forEach(accumulator -> accumulator.accumulate(store, context, record, message));
    }

    public List<Object> finish() {
        return accumulators.stream().map(RecordValueAccumulator::finish).collect(Collectors.toList());
    }
}
