/*
 * TupleConstructorValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class TupleConstructorValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Tuple-Value");
    @Nonnull
    protected final String functionName;
    @Nonnull
    protected final List<? extends Value> children;

    public TupleConstructorValue(@Nonnull List<? extends Value> children) {
        this("tuple", children);
    }

    protected TupleConstructorValue(@Nonnull String functionName,
                                    @Nonnull List<? extends Value> children) {
        this.functionName = functionName;
        this.children = ImmutableList.copyOf(children);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return new Type.Tuple(children.stream().map(Value::getResultType).collect(ImmutableList.toImmutableList()));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        return children.stream()
                .map(child -> child.eval(store, context, record, message))
                .collect(Collectors.toList());
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, functionName, children);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, functionName, children);
    }

    @Override
    public String toString() {
        return functionName + "(" + children.stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Nonnull
    @Override
    public TupleConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
        return new TupleConstructorValue(this.functionName, ImmutableList.copyOf(newChildren));
    }

    public static List<? extends Value> tryUnwrapIfTuple(@Nonnull final List<? extends Value> values) {
        if (values.size() != 1) {
            return values;
        }

        final Value onlyElement = Iterables.getOnlyElement(values);
        if (!(onlyElement instanceof TupleConstructorValue)) {
            return values;
        }

        return ImmutableList.copyOf(onlyElement.getChildren());
    }

    @AutoService(BuiltInFunction.class)
    public static class TupleFn extends BuiltInFunction<Value> {
        public TupleFn() {
            super("tuple",
                    ImmutableList.of(), new Type.Any(), TupleFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Atom> arguments) {
            final ImmutableList<Value> argumentValues = arguments.stream()
                    .peek(typed -> Preconditions.checkArgument(typed instanceof Value))
                    .map(typed -> ((Value)typed))
                    .collect(ImmutableList.toImmutableList());
            return new TupleConstructorValue(builtInFunction.getFunctionName(), argumentValues);
        }
    }
}
