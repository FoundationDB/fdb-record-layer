/*
 * OrderFunctionKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleOrdering;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import org.jspecify.annotations.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * {@code ORDER_xxx} function.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class OrderFunctionKeyExpression extends InvertibleFunctionKeyExpression implements QueryableKeyExpression {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Order-Function-Key-Expression");

    private final TupleOrdering.Direction direction;

    protected OrderFunctionKeyExpression(TupleOrdering.Direction direction,
                                         String name, KeyExpression arguments) {
        super(name, arguments);
        this.direction = direction;
    }

    public TupleOrdering.Direction getDirection() {
        return direction;
    }

    @Override
    public int getMinArguments() {
        return 1;
    }

    @Override
    public int getMaxArguments() {
        return 1;
    }

    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                    @Nullable Message message,
                                                                    Key.Evaluated arguments) {
        return Collections.singletonList(Key.Evaluated.scalar(ZeroCopyByteString.wrap(TupleOrdering.pack(arguments.toTuple(), direction))));
    }

    @Override
    public boolean createsDuplicates() {
        return getArguments().createsDuplicates();
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(final KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }

    @Override
    public Value toValue(final List<? extends Value> argumentValues) {
        return new ToOrderedBytesValue(argumentValues.get(0), direction);
    }

    @Nullable
    @Override
    public Function<Object, Object> getComparandConversionFunction() {
        return o -> ZeroCopyByteString.wrap(TupleOrdering.pack(Tuple.from(o), direction));
    }

    @Override
    public int planHash(final PlanHashable.PlanHashMode mode) {
        return super.basePlanHash(mode, BASE_HASH, direction);
    }

    @Override
    protected List<Key.Evaluated> evaluateInverseInternal(Key.Evaluated result) {
        return Collections.singletonList(Key.Evaluated.fromTuple(TupleOrdering.unpack(result.getObject(0, byte[].class), direction)));
    }

    @Override
    public boolean isInjective() {
        return true;
    }
}
