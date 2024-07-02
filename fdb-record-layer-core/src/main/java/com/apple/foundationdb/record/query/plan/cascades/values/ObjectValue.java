/*
 * ObjectValue.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PObjectValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link Value} representing any object.
 * <br>
 * For example, this is used to represent non-quantifiable data (e.g. within an expression).
 */
@API(API.Status.EXPERIMENTAL)
public class ObjectValue extends AbstractValue implements LeafValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Object-Value");

    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final Type resultType;

    private ObjectValue(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
        this.alias = alias;
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of(alias);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return ObjectValue.of(targetAlias, resultType);
    }

    @Nonnull
    @Override
    public Value replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        throw new RecordCoreException("this method should not be replaced with this method call.");
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return context.getBinding(alias);
    }

    @Nonnull
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return formatter.getQuantifierName(alias);
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
        return alias.toString();
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
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return false;
    }

    @Nonnull
    @Override
    public PObjectValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PObjectValue.newBuilder()
                .setAlias(getAlias().getId())
                .setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setObjectValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PObjectValue objectValueProto) {
        return new ObjectValue(CorrelationIdentifier.of(Objects.requireNonNull(objectValueProto.getAlias())), Type.fromTypeProto(serializationContext, Objects.requireNonNull(objectValueProto.getResultType())));
    }

    @Nonnull
    public static ObjectValue of(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
        return new ObjectValue(alias, resultType);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PObjectValue, ObjectValue> {
        @Nonnull
        @Override
        public Class<PObjectValue> getProtoMessageClass() {
            return PObjectValue.class;
        }

        @Nonnull
        @Override
        public ObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PObjectValue objectValueProto) {
            return ObjectValue.fromProto(serializationContext, objectValueProto);
        }
    }
}
