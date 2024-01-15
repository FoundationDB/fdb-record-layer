/*
 * PlanSerialization.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.serialization;

import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordQueryPlanProto;
import com.apple.foundationdb.record.RecordQueryPlanProto.PComparableObject;
import com.apple.foundationdb.record.RecordQueryPlanProto.PEnumLightValue;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.EnumBiMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Internal;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Class to facilitate serialization and deserialization of
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s.
 */
public class PlanSerialization {
    @Nonnull
    public static PComparableObject valueObjectToProto(@Nullable final Object object) {
        final PComparableObject.Builder builder = PComparableObject.newBuilder();
        if (object instanceof Internal.EnumLite) {
            builder.setEnumObject(PEnumLightValue.newBuilder()
                            .setName(object.toString()).setNumber(((Internal.EnumLite)object).getNumber()));
        } else if (object instanceof UUID) {
            final UUID uuid = (UUID)object;
            builder.setUuid(RecordQueryPlanProto.PUUID.newBuilder()
                    .setMostSigBits(uuid.getMostSignificantBits())
                    .setLeastSigBits(uuid.getLeastSignificantBits()))
                    .build();
        } else if (object instanceof FDBRecordVersion) {
            builder.setFdbRecordVersion(RecordQueryPlanProto.PFDBRecordVersion.newBuilder()
                    .setRawBytes(ByteString.copyFrom(((FDBRecordVersion)object).toBytes(false))).build());
        } else {
            builder.setPrimitiveObject(LiteralKeyExpression.toProtoValue(object));
        }
        return builder.build();
    }

    @Nullable
    public static Object protoObjectToValue(@Nonnull final PComparableObject proto) {
        if (proto.hasEnumObject()) {
            final PEnumLightValue enumProto = Objects.requireNonNull(proto.getEnumObject());
            Verify.verify(enumProto.hasNumber());
            return new ProtoUtils.DynamicEnum(enumProto.getNumber(), Objects.requireNonNull(enumProto.getName()));
        } else if (proto.hasUuid()) {
            final RecordQueryPlanProto.PUUID uuidProto = Objects.requireNonNull(proto.getUuid());
            return new UUID(uuidProto.getMostSigBits(), uuidProto.getLeastSigBits());
        } else if (proto.hasFdbRecordVersion()) {
            final RecordQueryPlanProto.PFDBRecordVersion fdbRecordVersion = Objects.requireNonNull(proto.getFdbRecordVersion());
            return FDBRecordVersion.fromBytes(fdbRecordVersion
                    .getRawBytes().toByteArray(), false);
        }
        return LiteralKeyExpression.fromProtoValue(Objects.requireNonNull(proto.getPrimitiveObject()));
    }

    @Nonnull
    public static Any protoObjectToAny(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final Message proto) {
        return Any.pack(proto, serializationContext.getRegistry().getTypeUrlPrefix());
    }

    @Nonnull
    public static Object dispatchFromProtoContainer(@Nonnull PlanSerializationContext serializationContext, @Nonnull final Message message) {
        final Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();
        Verify.verify(allFields.size() == 1);
        final Message field = (Message)Iterables.getOnlyElement(allFields.values());
        return PlanSerialization.dispatchFromProto(serializationContext, field);
    }

    @Nonnull
    public static Object dispatchFromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull Message message) {
        final PlanSerializationRegistry registry = serializationContext.getRegistry();
        if (message instanceof Any) {
            final Any any = (Any)message;
            final Class<? extends Message> messageClass = registry.lookUpMessageClass(any.getTypeUrl());
            try {
                message = any.unpack(messageClass);
            } catch (InvalidProtocolBufferException e) {
                throw new RecordCoreException("corrupt any field", e);
            }
        }
        return invokeDeserializer(serializationContext, message);
    }

    @SuppressWarnings("unchecked")
    private static <M extends Message> Object invokeDeserializer(@Nonnull final PlanSerializationContext serializationContext,
                                                                 @Nonnull final M message) {
        final PlanDeserializer<M, ?> deserializer =
                serializationContext.getRegistry().lookUpFromProto((Class<M>)message.getClass());
        return deserializer.fromProto(serializationContext, message);
    }

    @Nonnull
    public static <M extends Message, T> T getFieldOrThrow(@Nonnull M message,
                                                           @Nonnull final Predicate<M> fieldSetPredicate,
                                                           @Nonnull final Function<M, T> fieldExtractor) {
        if (fieldSetPredicate.test(message)) {
            return fieldExtractor.apply(message);
        }
        throw new RecordCoreException("Field is expected to be set but message does not have field.");
    }

    @Nullable
    public static <M extends Message, T> T getFieldOrNull(@Nonnull M message,
                                                          @Nonnull final Predicate<M> fieldSetPredicate,
                                                          @Nonnull final Function<M, T> fieldExtractor) {
        if (fieldSetPredicate.test(message)) {
            return fieldExtractor.apply(message);
        }
        return null;
    }

    @Nonnull
    public static <E1 extends Enum<E1>, E2 extends Enum<E2> & ProtocolMessageEnum> BiMap<E1, E2> protoEnumBiMap(@Nonnull final Class<E1> javaEnumClass,
                                                                                                                @Nonnull final Class<E2> protoEnumClass) {
        Verify.verify(javaEnumClass.isEnum());
        Verify.verify(protoEnumClass.isEnum());
        final E1[] javaEnumConstants = javaEnumClass.getEnumConstants();
        final E2[] protoEnumConstants = protoEnumClass.getEnumConstants();
        Verify.verify(javaEnumConstants.length == protoEnumConstants.length);

        final EnumBiMap<E1, E2> enumBiMap = EnumBiMap.create(javaEnumClass, protoEnumClass);
        for (int i = 0; i < javaEnumConstants.length; i ++) {
            final E1 e1 = javaEnumConstants[i];
            final E2 e2 = protoEnumConstants[i];
            Verify.verify(e1.ordinal() == e2.ordinal());
            Verify.verify(e1.name().equals(e2.name()));
            enumBiMap.put(e1, e2);
        }
        return enumBiMap;
    }
}
