/*
 * AggregateValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A value representing an aggregate: a value calculated (derived) from other values by applying an aggregation operator
 * (e.g. SUM, MIN, AVG etc.).
 * This class (as all {@link Value}s) is stateless. It is used to evaluate and return runtime results based on the
 * given parameters.
 *
 */
public interface AggregateValue extends Value {
    /**
     * Create a new accumulator from the Value. This accumulator will perform state management and accumulate evaluated
     * values representing this value.
     *
     * @param typeRepository dynamic schema
     *
     * @return a new {@link Accumulator} for aggregating values for this Value.
     */
    @Nonnull
    Accumulator createAccumulator(@Nonnull TypeRepository typeRepository);

    @Nullable
    <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context);

    @Nonnull
    @Override
    default AggregateValue translateCorrelations(@Nonnull final TranslationMap translationMap) {
        return (AggregateValue)Value.super.translateCorrelations(translationMap);
    }
}
