/*
 * QueriedValue.java
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
import com.apple.foundationdb.record.planprotos.PQueriedValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A value representing the source of a value derivation.
 */
@API(API.Status.EXPERIMENTAL)
public class QueriedValue extends AbstractValue implements LeafValue, Value.CompileTimeValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Queried-Value");

    @Nonnull
    private final Type resultType;

    @Nullable
    private List<String> recordTypeNames;

    public QueriedValue() {
        this(Type.primitiveType(Type.TypeCode.UNKNOWN));
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    public QueriedValue(@Nonnull final Type resultType) {
        this.resultType = resultType;
    }

    public QueriedValue(@Nonnull final Type resultType, @Nullable final Iterable<String> recordTypeNames) {
        this.resultType = resultType;
        this.recordTypeNames = recordTypeNames == null ? null : ImmutableList.copyOf(recordTypeNames);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    public List<String> getRecordTypeNames() {
        return recordTypeNames;
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

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "base()";
    }

    @Override
    public String toString() {
        if (recordTypeNames == null) {
            return "base()";
        }
        return "base(" + String.join(",", recordTypeNames) + ")";
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
    public PQueriedValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PQueriedValue.Builder builder =  PQueriedValue.newBuilder()
                .setResultType(resultType.toTypeProto(serializationContext));
        builder.setHasRecordTypeNames(recordTypeNames != null);
        if (recordTypeNames != null) {
            builder.addAllRecordTypeNames(recordTypeNames);
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setQueriedValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static QueriedValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PQueriedValue queriedValueProto) {
        Verify.verify(queriedValueProto.hasHasRecordTypeNames());
        final List<String> recordTypeNames;
        if (queriedValueProto.getHasRecordTypeNames()) {
            final ImmutableList.Builder<String> recordTypeNamesBuilder = ImmutableList.builder();
            for (int i = 0; i < queriedValueProto.getRecordTypeNamesCount(); i ++) {
                recordTypeNamesBuilder.add(queriedValueProto.getRecordTypeNames(i));
            }
            recordTypeNames = recordTypeNamesBuilder.build();
        } else {
            recordTypeNames = null;
        }
        return new QueriedValue(Type.fromTypeProto(serializationContext, Objects.requireNonNull(queriedValueProto.getResultType())),
                recordTypeNames);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PQueriedValue, QueriedValue> {
        @Nonnull
        @Override
        public Class<PQueriedValue> getProtoMessageClass() {
            return PQueriedValue.class;
        }

        @Nonnull
        @Override
        public QueriedValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PQueriedValue queriedValueProto) {
            return QueriedValue.fromProto(serializationContext, queriedValueProto);
        }
    }
}
