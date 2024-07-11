/*
 * RecordTypeValue.java
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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecordTypeValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A value which is unique for each record type produced by its quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordTypeValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("RecordType-Value");

    @Nonnull
    private final Value in;

    public RecordTypeValue(@Nonnull final Value in) {
        this.in = in;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var inRecord = in.eval(store, context);
        if (!(inRecord instanceof Message)) {
            return null;
        }

        return store.getRecordMetaData().getRecordType(((M)inRecord).getDescriptorForType().getName()).getRecordTypeKey();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        return new RecordTypeValue(Iterables.getOnlyElement(newChildren));
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
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.LONG); // eval returns a Tuple-friendly record type key which is Long.
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return toString();
    }

    @Override
    public String toString() {
        return "recordType(" + in + ")";
    }

    @Nonnull
    @Override
    public PRecordTypeValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordTypeValue.newBuilder()
                .setIn(in.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRecordTypeValue(toProto(serializationContext)).build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static RecordTypeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PRecordTypeValue recordTypeValueProto) {
        if (recordTypeValueProto.hasAlias()) {
            return new RecordTypeValue(
                    QuantifiedObjectValue.of(
                            CorrelationIdentifier.of(Objects.requireNonNull(recordTypeValueProto.getAlias())),
                            new Type.AnyRecord(true)));
        } else {
            return new RecordTypeValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(recordTypeValueProto.getIn())));

        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordTypeValue, RecordTypeValue> {
        @Nonnull
        @Override
        public Class<PRecordTypeValue> getProtoMessageClass() {
            return PRecordTypeValue.class;
        }

        @Nonnull
        @Override
        public RecordTypeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PRecordTypeValue recordTypeValueProto) {
            return RecordTypeValue.fromProto(serializationContext, recordTypeValueProto);
        }
    }
}
