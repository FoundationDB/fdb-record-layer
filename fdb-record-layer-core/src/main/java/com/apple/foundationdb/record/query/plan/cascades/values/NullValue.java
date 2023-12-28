/*
 * NullValue.java
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
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.RecordQueryPlanProto;
import com.apple.foundationdb.record.RecordQueryPlanProto.PNullValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.ProtoMessage;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A value that evaluates to empty.
 */
@API(API.Status.EXPERIMENTAL)
@AutoService(PlanSerializable.class)
@ProtoMessage(PNullValue.class)
public class NullValue extends AbstractValue implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Null-Value");

    @Nonnull
    private final Type resultType;

    public NullValue(@Nonnull final Type resultType) {
        this.resultType = resultType.nullable();
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return null;
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
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!LeafValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }

        return resultType.equals(other.getResultType());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                // We do hash result type here as the type (not being of type NULL) is always the direct result of
                // type inference (target in an UPDATE or similar) or an explicit cast.
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, resultType);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "null";
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

    @Override
    public boolean canResultInType(@Nonnull final Type type) {
        if (!type.isNullable()) {
            return false;
        }
        return resultType.isUnresolved() || resultType.getTypeCode() == Type.TypeCode.ANY;
    }

    @Nonnull
    @Override
    public Value with(@Nonnull final Type type) {
        Verify.verify(type.isNullable());
        return new NullValue(type);
    }

    @Nonnull
    @Override
    public PNullValue toProto(@Nonnull final PlanHashMode mode) {
        return PNullValue.newBuilder()
                .setResultType(resultType.toTypeProto(mode))
                .build();
    }

    @Nonnull
    @Override
    public RecordQueryPlanProto.PValue toValueProto(@Nonnull final PlanHashMode mode) {
        return RecordQueryPlanProto.PValue.newBuilder().setNullValue(toProto(mode)).build();
    }

    @Nonnull
    public static NullValue fromProto(@Nonnull final PlanHashMode mode, @Nonnull final PNullValue nullValueProto) {
        return new NullValue(Type.fromTypeProto(mode, Objects.requireNonNull(nullValueProto.getResultType())));
    }
}
