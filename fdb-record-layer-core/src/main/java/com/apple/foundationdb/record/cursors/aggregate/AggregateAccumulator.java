/*
 * AggregateAccumulator.java
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
import java.util.List;

/**
 * This interface encapsulates the aggregation operator behavior: It holds onto the current state
 * and applies a new record onto the current state to arrive at the new accumulated state.
 * The {@link #accumulate} method is used to evaluate a {@link Value} and to accumulate the result into the running aggregate.
 * Since the {@link Value} is stateless, this class is used to hold the aggregated state, applying the aggregation methods as necessary.
 * The {@link #finish()} method is used to finalize the aggregation flow and return the result.
 */
public interface AggregateAccumulator {
    /**
     * Apply a new record on top of the current aggregation.
     *
     * @param store the record store to use in the value evaluation
     * @param context the evaluation context, used to evaluate the value
     * @param record the record containing the message, used to evaluate the value
     * @param message the message containing the result that needs to be accumulated into the aggregation
     * @param <M> the type of records used in the evaluation
     */
    <M extends Message> void accumulate(@Nonnull FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                        @Nullable final FDBRecord<M> record, @Nonnull final M message);

    /**
     * Calculate and return the result from the accumulated state.
     *
     * @return the calculated results accumulated.
     */
    List<Object> finish();
}
