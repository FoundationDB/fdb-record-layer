/*
 * ArraySortValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PArraySortValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A value that returns a copy of its child array {@link Value} with its elements sorted in ascending or, if reverse,
 * descending order. The elements must be comparable with each other.
 */
@API(API.Status.EXPERIMENTAL)
public class ArraySortValue extends AbstractValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Array-Sort-Value");

    @SuppressWarnings("unchecked")
    private static final Comparator<Object> ELEMENT_COMPARATOR = Comparator.comparing(Comparable.class::cast);

    @Nonnull
    private final Value childValue;
    private final boolean reverse;

    public ArraySortValue(@Nonnull final Value childValue, final boolean reverse) {
        final Type childType = Objects.requireNonNull(childValue.getResultType());
        Verify.verify(childType.isArray());
        final Type elementType = ((Type.Array)childType).getElementType();
        Verify.verify(elementType == null || isSortable(elementType), "cannot sort elements of type %s", elementType);
        this.childValue = childValue;
        this.reverse = reverse;
    }

    /**
     * Whether elements of a type can be sorted, which needs their representation to be {@link Comparable}.
     */
    private static boolean isSortable(@Nonnull final Type elementType) {
        if (elementType.isEnum()) {
            return true;
        }
        return elementType.isPrimitive() &&
                elementType.getTypeCode() != Type.TypeCode.BYTES && elementType.getTypeCode() != Type.TypeCode.VECTOR;
    }

    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    @Override
    public List<? extends Value> computeChildren() {
        return ImmutableList.of(childValue);
    }

    @Nonnull
    @Override
    public Value getChild() {
        return childValue;
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
        if (getChild() == rebasedChild) {
            return this;
        }
        return new ArraySortValue(rebasedChild, reverse);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return childValue.getResultType();
    }

    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var childResult = childValue.eval(store, context);
        if (childResult == null) {
            return null;
        }
        return sort((List<?>)childResult, reverse);
    }

    /**
     * Sorts a list of comparable elements.
     * @param elements the elements to sort
     * @param reverse whether to sort in descending order
     * @return a sorted copy of the elements
     */
    @Nonnull
    public static List<Object> sort(@Nonnull final List<?> elements, final boolean reverse) {
        final List<Object> sorted = new ArrayList<>(elements);
        sorted.sort(reverse ? ELEMENT_COMPARATOR.reversed() : ELEMENT_COMPARATOR);
        return sorted;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, reverse);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, childValue, reverse);
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> reverse == ((ArraySortValue)other).isReverse());
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var explainTokens = new ExplainTokens().addFunctionCall("arraySort",
                Value.explainFunctionArguments(explainSuppliers));
        if (reverse) {
            explainTokens.addWhitespace().addKeyword("DESC");
        }
        return ExplainTokensWithPrecedence.of(explainTokens);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PArraySortValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PArraySortValue.newBuilder()
                .setChildValue(childValue.toValueProto(serializationContext))
                .setReverse(reverse)
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setArraySortValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ArraySortValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PArraySortValue arraySortValueProto) {
        return new ArraySortValue(
                Value.fromValueProto(serializationContext, Objects.requireNonNull(arraySortValueProto.getChildValue())),
                arraySortValueProto.getReverse());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PArraySortValue, ArraySortValue> {
        @Nonnull
        @Override
        public Class<PArraySortValue> getProtoMessageClass() {
            return PArraySortValue.class;
        }

        @Nonnull
        @Override
        public ArraySortValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PArraySortValue arraySortValueProto) {
            return ArraySortValue.fromProto(serializationContext, arraySortValueProto);
        }
    }
}
