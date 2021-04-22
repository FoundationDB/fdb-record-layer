/*
 * BaseAggregateValue.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.function.Function;

/**
 * A Base class that encapsulates the logic of a {@link AggregateValue}.
 * This class is a stateless calculator for aggregate values, parsing the next values from the given inner values.
 *
 * @param <S> the type of the state maintained vy this aggregate value
 * @param <T> the type of operand that this class aggregates
 * <p>
 * In most cases, S and T are the same. In some cases, though (See Average for example), the state has different type
 * than the operand, and there is a calculation that needs to be made to derive the result from the state.
 */
public abstract class BaseAggregateValue<S, T> implements AggregateValue<S> {
    // The field value on which this aggregate is working. This represents the field value that we can evaluate to get to the numeric
    // value we need.
    @Nonnull
    protected Value inner;
    @Nonnull
    private final S initial; // The initial value for the aggregation
    @Nonnull
    private final ObjectPlanHash baseHash;
    @Nonnull
    private final Function<Value, Value> withChildrenOp; // The function to call when need to construct a new instance with new children

    protected BaseAggregateValue(@Nonnull final Value inner, @Nonnull final S initial, @Nonnull final String hashObjectName,
                                 @Nonnull Function<Value, Value> withChildrenOp) {
        this.inner = inner;
        this.initial = initial;
        this.baseHash = new ObjectPlanHash(hashObjectName);
        this.withChildrenOp = withChildrenOp;
    }

    @Nonnull
    @Override
    public S initial() {
        return initial;
    }

    @Nonnull
    @Override
    public <M extends Message> S accumulate(@Nonnull final S currentState, @Nonnull final FDBRecordStoreBase<M> store,
                                            @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record,
                                            @Nonnull final M message) {
        // This assumes that the Value is returning the right type (no way to prove at compile time).
        T val = (T)inner.eval(store, context, record, message);
        return accumulate(currentState, val);
    }

    @Nonnull
    protected abstract S accumulate(S currentState, T next);

    @Override
    public int semanticHashCode() {
        // todo
        return 0;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, baseHash, inner);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return Collections.singletonList(inner);
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        return withChildrenOp.apply(Iterables.getOnlyElement(newChildren));
    }
}