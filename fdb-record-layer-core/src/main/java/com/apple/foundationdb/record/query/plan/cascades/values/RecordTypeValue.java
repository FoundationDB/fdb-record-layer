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
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * A value which is unique for each record type produced by its quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordTypeValue extends AbstractValue implements QuantifiedValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("RecordType-Value");

    @Nonnull
    private final CorrelationIdentifier alias;

    public RecordTypeValue(@Nonnull final CorrelationIdentifier alias) {
        this.alias = alias;
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return new RecordTypeValue(targetAlias);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final QueryResult binding = (QueryResult)context.getBinding(alias);

        final var messageOptional = binding.getMessageMaybe();
        if (!(binding instanceof Message)) {
            return null;
        }

        return messageOptional.map(message ->
                        store.getRecordMetaData().getRecordType(message.getDescriptorForType().getName()).getRecordTypeKey())
                .orElse(null);
    }

    @Override
    @Nonnull
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return QuantifiedValue.super.getCorrelatedToWithoutChildren();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
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
        return semanticEquals(other, AliasMap.identitiesFor(ImmutableSet.of(alias)));
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        if (otherValue instanceof QuantifiedValue) {
            return getAlias().equals(((QuantifiedValue)otherValue).getAlias());
        }
        return false;
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
        return "recordType(" + alias + ")";
    }

    @Nonnull
    @Override
    public PRecordTypeValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordTypeValue.newBuilder().setAlias(alias.getId()).build();
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
        return new RecordTypeValue(CorrelationIdentifier.of(Objects.requireNonNull(recordTypeValueProto.getAlias())));
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
