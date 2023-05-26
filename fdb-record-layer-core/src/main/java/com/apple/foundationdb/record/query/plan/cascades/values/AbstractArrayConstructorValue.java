/*
 * AbstractArrayConstructorValue.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A {@link Value} that encapsulates its homogeneous children {@link Value}s into a single
 * {@link Type.Array} typed {@link Value}.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AbstractArrayConstructorValue extends AbstractValue implements CreatesDynamicTypesValue {
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
    public Collection<? extends Value> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "[" + children.stream().map(child -> child.explain(formatter)).collect(Collectors.joining(", ")) + "]";
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH);
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
    private static Value encapsulateInternal(@Nonnull final List<? extends Typed> typedArgs) {
        final ImmutableList<Value> arguments = typedArgs.stream()
                .map(typedArg -> {
                    Verify.verify(typedArg.getResultType().getTypeCode() != Type.TypeCode.RELATION && typedArg instanceof Value);
                    return (Value)typedArg; } )
                .collect(ImmutableList.toImmutableList());

        Verify.verify(!typedArgs.isEmpty());
        final Type elementType = resolveElementType(arguments);
        return new LightArrayConstructorValue(injectPromotions(arguments, elementType), elementType);
    }

    @Nonnull
    private static Type resolveElementType(@Nonnull final Iterable<? extends Typed> argumentTypeds) {
        final var resolvedType = StreamSupport.stream(argumentTypeds.spliterator(), false)
                .map(Typed::getResultType)
                .reduce(null, (l, r) -> {
                    if (l == null) {
                        return r;
                    }
                    Verify.verifyNotNull(r);

                    return Type.maximumType(l, r);
                });
        SemanticException.check(resolvedType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        return resolvedType;
    }

    @Nonnull
    private static List<? extends Value> injectPromotions(@Nonnull Iterable<? extends Value> children, @Nonnull final Type elementType) {
        return Streams.stream(children)
                .map(child -> PromoteValue.inject(child, elementType))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * An array that does not need any nested proto constructed.
     */
    @SuppressWarnings("java:S2160")
    public static class LightArrayConstructorValue extends AbstractArrayConstructorValue {
        private LightArrayConstructorValue(@Nonnull final List<? extends Value> children) {
            this(children, AbstractArrayConstructorValue.resolveElementType(children));
        }

        private LightArrayConstructorValue(@Nonnull final Type elementType) {
            this(ImmutableList.of(), elementType);
        }

        private LightArrayConstructorValue(@Nonnull final List<? extends Value> children, @Nonnull final Type elementType) {
            super(children, elementType);
        }

        @Nullable
        @Override
        @SuppressWarnings("java:S6213")
        public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            return getChildren().stream()
                    .map(child -> child.eval(store, context))
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        @Override
        public LightArrayConstructorValue withChildren(final Iterable<? extends Value> newChildren) {
            if (Iterables.isEmpty(newChildren)) {
                return this;
            }
            Verify.verify(resolveElementType(newChildren).equals(getElementType()));
            return new LightArrayConstructorValue(AbstractArrayConstructorValue.injectPromotions(newChildren, getElementType()), getElementType());
        }

        @Nonnull
        @Override
        public boolean canBePromotedToType(@Nonnull final Type type) {
            if (!getChildren().isEmpty()) {
                return false;
            }
            return type.isUnresolved();
        }

        @Nonnull
        @Override
        public Value promoteToType(@Nonnull final Type type) {
            Verify.verify(getChildren().isEmpty());
            return emptyArray(type); // only empty arrays are currently promotable
        }

        @Nonnull
        public static LightArrayConstructorValue of(@Nonnull final Value value1, @Nonnull final Value... valuesN) {
            final var children = ImmutableList.<Value>builder().add(value1).add(valuesN).build();
            return of(children);
        }

        @Nonnull
        public static LightArrayConstructorValue of(@Nonnull final List<? extends Value> children) {
            Verify.verify(!children.isEmpty());
            return new LightArrayConstructorValue(children);
        }

        @Nonnull
        public static LightArrayConstructorValue emptyArray(@Nonnull final Type elementType) {
            return new LightArrayConstructorValue(elementType);
        }
    }

    /**
     * The {@code array} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class ArrayFn extends BuiltInFunction<Value> {
        public ArrayFn() {
            super("array",
                    ImmutableList.of(), new Type.Any(), (builtInFunction, typedArgs) -> encapsulateInternal(typedArgs));
        }
    }
}
