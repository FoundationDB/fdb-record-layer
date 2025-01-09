/*
 * UdfValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This represents a user-defined {@code Value} that can be subclassed to provide a custom implementation.
 * To do this, the implementor is expected to:
 * <ul>
 *     <li>Subclass of {@link UdfFunction} that provides an implementation describing the UDF such as its name (which is the name of
 *     the class itself), the UDF parameter types, and the instantiation rules of a corresponding subclass of {@link UdfValue},
 *     for example, type promotion rules, calculated return type, and so on.</li>
 *     <li>Subclass of {@link UdfValue} that represents the actual UDF business logic, this value can be either stateless
 *     or stateful which could be useful for e.g. calculating an aggregation, the implementor supplies the implementation
 *     in {@link UdfValue#call(List)} method, this method expects zero or more arguments, the number and type of these arguments
 *     is determined by overriding {@link UdfFunction#getParameterTypes()} in the corresponding {@link UdfFunction}.</li>
 * </ul>
 */
public abstract class UdfValue extends AbstractValue {

    @Nonnull
    private final Iterable<? extends Value> children;

    @Nonnull
    private final Type resultType;

    public UdfValue(@Nonnull final Iterable<? extends Value> children, @Nonnull final Type resultType) {
        this.children = children;
        this.resultType = resultType;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, this.getClass().getCanonicalName(), children);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Override
    @Nonnull
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return call(StreamSupport.stream(children.spliterator(), false).map(c -> c.eval(store, context)).collect(Collectors.toList()));
    }

    @Nonnull
    @Override
    public abstract Value withChildren(Iterable<? extends Value> newChildren);

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall(getClass().getSimpleName(),
                Value.explainFunctionArguments(explainSuppliers)));
    }

    @Nullable
    public abstract Object call(@Nonnull List<Object> arguments);

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, this.getClass().getCanonicalName());
    }
}
