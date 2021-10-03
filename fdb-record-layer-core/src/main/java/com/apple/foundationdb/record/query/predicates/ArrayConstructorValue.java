/*
 * ArrayConstructorValue.java
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
import com.apple.foundationdb.record.query.predicates.Type.TypeCode;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class ArrayConstructorValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Array-Constructor-Value");

    @Nonnull
    private final List<? extends Value> children;

    @Nonnull
    private final Type elementType;

    private ArrayConstructorValue(@Nonnull final List<? extends Value> children, @Nonnull final Type elementType) {
        this.elementType = elementType;
        this.children = ImmutableList.copyOf(children);
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        return children.stream()
                .map(child -> child.eval(store, context, record, message))
                .collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "array(" + children.stream().map(child -> child.explain(formatter)).collect(Collectors.joining(", ")) + ")";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return new Type.Collection(elementType);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public ArrayConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(!Iterables.isEmpty(newChildren));
        Verify.verifyNotNull(resolveElementType(newChildren).equals(elementType));
        return new ArrayConstructorValue(ImmutableList.copyOf(newChildren), elementType);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, children);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, children);
    }

    @Override
    public String toString() {
        return "array(" + children.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
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
    @SuppressWarnings("ConstantConditions")
    private static Value encapsulate(@Nonnull ParserContext parserContext,
                                     @Nonnull BuiltInFunction<Value> builtInFunction,
                                     @Nonnull final List<Atom> argumentAtoms) {
        final ImmutableList<Value> arguments = argumentAtoms.stream()
                .peek(argumentAtom -> Verify.verify(argumentAtom.getResultType().getTypeCode() != TypeCode.STREAM && argumentAtom instanceof Value))
                .map(argumentAtom -> (Value)argumentAtom)
                .collect(ImmutableList.toImmutableList());

        final Type elementType = resolveElementType(argumentAtoms);

        return new ArrayConstructorValue(arguments, elementType);
    }

    private static Type resolveElementType(@Nonnull final Iterable<? extends Atom> argumentAtoms) {
        return StreamSupport.stream(argumentAtoms.spliterator(), false)
                .map(Atom::getResultType)
                .reduce(new Type.Any(), (l, r) -> {
                    if (l == null || r == null) {
                        throw new IllegalStateException("should not be here");
                    }

                    if (l instanceof Type.Any) {
                        return r;
                    }

                    if (r instanceof Type.Any) {
                        return l;
                    }

                    // TODO type promotion
                    Verify.verify(l.equals(r), "types of children must be equal");

                    return l;
                });
    }

    @AutoService(BuiltInFunction.class)
    public static class ArrayFn extends BuiltInFunction<Value> {
        public ArrayFn() {
            super("array",
                    ImmutableList.of(), new Type.Any(), ArrayConstructorValue::encapsulate);
        }
    }
}
