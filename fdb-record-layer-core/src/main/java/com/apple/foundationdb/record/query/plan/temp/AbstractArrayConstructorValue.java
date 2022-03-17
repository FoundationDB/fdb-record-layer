/*
 * AbstractArrayConstructorValue.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.plan.temp.Type.Array.needsNestedProto;

/**
 * A {@link Value} that encapsulates its homogeneous children {@link Value}s into a single
 * {@link com.apple.foundationdb.record.query.plan.temp.Type.Array} typed {@link Value}.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AbstractArrayConstructorValue implements Value, CreatesDynamicTypesValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Array-Constructor-Value");

    @Nonnull
    private final List<? extends Value> children;

    @Nonnull
    private final Type elementType;

    protected AbstractArrayConstructorValue(@Nonnull final List<? extends Value> children, @Nonnull final Type elementType) {
        this.elementType = elementType;
        this.children = ImmutableList.copyOf(children);
    }

    @Nonnull
    public Type getElementType() {
        return elementType;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return new Type.Array(elementType);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "(" + children.stream().map(child -> child.explain(formatter)).collect(Collectors.joining(", ")) + ") as Array";
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
    private static Value encapsulateInternal(@Nonnull final List<Typed> typedArgs) {
        final ImmutableList<Value> arguments = typedArgs.stream()
                .map(typedArg -> {
                    Verify.verify(typedArg.getResultType().getTypeCode() != Type.TypeCode.RELATION && typedArg instanceof Value);
                    return (Value)typedArg; } )
                .collect(ImmutableList.toImmutableList());

        final Type elementType = resolveElementType(arguments);

        if (needsNestedProto(elementType)) {
            return new ArrayConstructorValue(arguments, elementType);
        } else {
            return new LightArrayConstructorValue(arguments, elementType);
        }
    }

    private static Type resolveElementType(@Nonnull final Iterable<? extends Typed> argumentTypeds) {
        return StreamSupport.stream(argumentTypeds.spliterator(), false)
                .map(Typed::getResultType)
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

    @SuppressWarnings("java:S2160")
    public static class LightArrayConstructorValue extends AbstractArrayConstructorValue {
        public LightArrayConstructorValue(@Nonnull final List<? extends Value> children, @Nonnull final Type elementType) {
            super(children, elementType);
        }

        @Nullable
        @Override
        @SuppressWarnings("java:S6213")
        public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            return StreamSupport.stream(getChildren().spliterator(), false)
                    .map(child -> child.eval(store, context, record, message))
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        @Override
        public LightArrayConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
            Verify.verify(!Iterables.isEmpty(newChildren));
            Verify.verifyNotNull(resolveElementType(newChildren).equals(getElementType()));
            return new LightArrayConstructorValue(ImmutableList.copyOf(newChildren), getElementType());
        }
    }

    @SuppressWarnings("java:S2160")
    public static class ArrayConstructorValue extends AbstractArrayConstructorValue {

        public ArrayConstructorValue(@Nonnull final List<? extends Value> children, @Nonnull final Type elementType) {
            super(children, elementType);
        }

        @Nullable
        @Override
        @SuppressWarnings("java:S6213")
        public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            final DynamicMessage.Builder resultMessageBuilder = newMessageBuilderForType(context.getDynamicSchema());
            final Descriptors.Descriptor descriptorForType = resultMessageBuilder.getDescriptorForType();

            return StreamSupport.stream(getChildren().spliterator(), false)
                    .map(child -> {
                        final Object childResultElement = child.eval(store, context, record, message);
                        final DynamicMessage.Builder helperMessageBuilder = DynamicMessage.newBuilder(descriptorForType);
                        if (childResultElement != null) {
                            helperMessageBuilder.setField(descriptorForType.findFieldByNumber(1), childResultElement);
                        }
                        return helperMessageBuilder.build();
                    })
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        @Override
        public ArrayConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
            Verify.verify(!Iterables.isEmpty(newChildren));
            Verify.verifyNotNull(resolveElementType(newChildren).equals(getElementType()));
            return new ArrayConstructorValue(ImmutableList.copyOf(newChildren), getElementType());
        }

        @Nonnull
        private DynamicMessage.Builder newMessageBuilderForType(@Nonnull DynamicSchema dynamicSchema) {
            return Objects.requireNonNull(dynamicSchema.newMessageBuilder(getResultType()));
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class ArrayFn extends BuiltInFunction<Value> {
        public ArrayFn() {
            super("array",
                    ImmutableList.of(), new Type.Any(), (parserContext, builtInFunction, typedArgs) -> encapsulateInternal(typedArgs));
        }
    }
}
