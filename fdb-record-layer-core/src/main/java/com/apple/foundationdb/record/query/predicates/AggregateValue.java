/*
 * AggregateValue.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.plans.QueryResultElement;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A value representing an aggregate: a value calculated (derived) from other values by applying an aggregation operator (e.g.
 * SUM, MIN, AVG etc.).
 * This class (as all {@link Value}s) is stateless. It is used to evaluate and return runtime results based on the given
 * parameters.
 *
 * @param <S> the state type. The state holds the interim results as they are being accumulated, and where the eventual result is derived from.
 */
public interface AggregateValue<S> extends Value.CompileTimeValue {
    /**
     * Return the initial state for the aggregation. This is what the aggregation will start with.
     * @return the initial value for the aggregation
     */
    @Nonnull
    S initial();

    /**
     * Perform the aggregation operation. This will apply an additional result to the accumulated state so far.
     * @param currentState the so-far accumulated state
     * @param store the record store used to evaluate the value
     * @param context the evaluation context used to evaluate the value
     * @param record the record containing the message, used to evaluate the value
     * @param message the message containing the result that needs to be accumulated into the aggregation
     * @param <M> the type of records stored in the store
     * @return the new accumulated aggregated state
     */
    @Nonnull
    <M extends Message> S accumulate(@Nonnull S currentState, @Nonnull final FDBRecordStoreBase<M> store,
                                     @Nonnull final EvaluationContext context, @Nullable FDBRecord<M> record,
                                     @Nonnull M message);

    /**
     * Convert the accumulated state to a result. This would normally be done when the aggregation is complete and we need to
     * create a query result element that needs to flow onwards. The created result will contain the final aggregated value.
     * @param currentState the so-far accumulated state
     * @return the record that holds the final aggregated result
     */
    @Nonnull
    QueryResultElement finish(S currentState);
}
