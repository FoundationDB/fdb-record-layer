/*
 * IndexedValue.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PIndexedValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A value representing the source of a value derivation.
 */
@API(API.Status.EXPERIMENTAL)
public class IndexedValue extends AbstractValue implements LeafValue, Value.CompileTimeValue {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Indexed-Value");

    @Nonnull
    private final Type resultType;

    public IndexedValue() {
        this(Type.primitiveType(Type.TypeCode.UNKNOWN));
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    public IndexedValue(@Nonnull final Type resultType) {
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Value replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        return this;
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return false;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH);
    }

    @Override
    public String toString() {
        return "base()";
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
    public PIndexedValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexedValue.newBuilder().setResultType(resultType.toTypeProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setIndexedValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static IndexedValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PIndexedValue indexedValueProto) {
        return new IndexedValue(Type.fromTypeProto(serializationContext,
                Objects.requireNonNull(indexedValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PIndexedValue, IndexedValue> {
        @Nonnull
        @Override
        public Class<PIndexedValue> getProtoMessageClass() {
            return PIndexedValue.class;
        }

        @Nonnull
        @Override
        public IndexedValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PIndexedValue indexedValueProto) {
            return IndexedValue.fromProto(serializationContext, indexedValueProto);
        }
    }
}
