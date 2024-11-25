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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.planprotos.PComparableObject;
import com.apple.foundationdb.record.planprotos.PEnumLightValue;
import com.apple.foundationdb.record.planprotos.PFDBRecordVersion;
import com.apple.foundationdb.record.planprotos.PUUID;
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
@API(API.Status.INTERNAL)
public class PlanSerialization {
    /**
     * Helper method to serialize plan fragments that are essentially values of the underlying data model.
     * For instance a comparison {@code = 5} needs to be able to serialize the number {@code 5}. Unfortunately,
     * the kind of objects that can be used for comparisons, in value-lists, etc. is quite extensive. This method
     * should be able to deal with all kinds of objects that can appear in these cases. Note that enums do not
     * round-trip as the instance of the actual enum is not deserializable as such. Instead, we resort to deserializing
     * an enum as a {@link com.apple.foundationdb.record.util.ProtoUtils.DynamicEnum}.
     * @param object object that also happens to be a plan fragment
     * @return a {@link PComparableObject} that can be serialized.
     */
    @Nonnull
    public static PComparableObject valueObjectToProto(@Nullable final Object object) {
        final PComparableObject.Builder builder = PComparableObject.newBuilder();
        if (object instanceof Internal.EnumLite) {
            builder.setEnumObject(PEnumLightValue.newBuilder()
                            .setName(object.toString()).setNumber(((Internal.EnumLite)object).getNumber()));
        } else if (object instanceof UUID) {
            final UUID uuid = (UUID)object;
            builder.setUuid(PUUID.newBuilder()
                    .setMostSigBits(uuid.getMostSignificantBits())
                    .setLeastSigBits(uuid.getLeastSignificantBits()))
                    .build();
        } else if (object instanceof FDBRecordVersion) {
            builder.setFdbRecordVersion(PFDBRecordVersion.newBuilder()
                    .setRawBytes(ByteString.copyFrom(((FDBRecordVersion)object).toBytes(false))).build());
        } else if (object instanceof ByteString) {
            builder.setBytesAsByteString((ByteString)object);
        } else {
            builder.setPrimitiveObject(LiteralKeyExpression.toProtoValue(object));
        }
        return builder.build();
    }

    /**
     * Helper method to deserialize plan fragments that are essentially values of the underlying data model.
     * For instance a comparison {@code = 5} needs to be able to serialize the number {@code 5}. Unfortunately,
     * the kind of objects that can be used for comparisons, in value-lists, etc. is quite extensive. This method
     * should be able to deal with all kinds of objects that can appear in these cases. Note that enums do not
     * round-trip as the instance of the actual enum is not deserializable as such. Instead, we resort to deserializing
     * an enum as a {@link com.apple.foundationdb.record.util.ProtoUtils.DynamicEnum}.
     * @param proto a {@link PComparableObject} that can be deserialized
     * @return a value object
     */
    @Nullable
    public static Object protoToValueObject(@Nonnull final PComparableObject proto) {
        if (proto.hasEnumObject()) {
            final PEnumLightValue enumProto = Objects.requireNonNull(proto.getEnumObject());
            Verify.verify(enumProto.hasNumber());
            return new ProtoUtils.DynamicEnum(enumProto.getNumber(), Objects.requireNonNull(enumProto.getName()));
        } else if (proto.hasUuid()) {
            final PUUID uuidProto = Objects.requireNonNull(proto.getUuid());
            return new UUID(uuidProto.getMostSigBits(), uuidProto.getLeastSigBits());
        } else if (proto.hasFdbRecordVersion()) {
            final PFDBRecordVersion fdbRecordVersion = Objects.requireNonNull(proto.getFdbRecordVersion());
            return FDBRecordVersion.fromBytes(fdbRecordVersion
                    .getRawBytes().toByteArray(), false);
        } else if (proto.hasBytesAsByteString()) {
            return proto.getBytesAsByteString();
        }
        return LiteralKeyExpression.fromProtoValue(Objects.requireNonNull(proto.getPrimitiveObject()));
    }

    /**
     * Method that packs a {@link Message} passed in into an {@link Any} using the correct type url prefix.
     * @param serializationContext serialization context in use
     * @param proto a message
     * @return a new {@link Any} that holds {@code proto}
     */
    @Nonnull
    public static Any protoObjectToAny(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final Message proto) {
        return Any.pack(proto, serializationContext.getRegistry().getTypeUrlPrefix());
    }

    /**
     * Method that, given a protobuf container message (a P-message that contains a {@code oneof} of specific messages),
     * extracts the specific message, and dispatches to the {@link PlanDeserializer} that can deserialize the particular
     * specific message.
     * @param serializationContext serialization context
     * @param message the message to deserialize
     * @return a new object that is the plan fragment corresponding to the message passed in
     */
    @Nonnull
    public static Object dispatchFromProtoContainer(@Nonnull PlanSerializationContext serializationContext, @Nonnull final Message message) {
        final Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();
        Verify.verify(allFields.size() == 1);
        final Message field = (Message)Iterables.getOnlyElement(allFields.values());
        return PlanSerialization.dispatchFromProto(serializationContext, field);
    }

    @Nonnull
    private static Object dispatchFromProto(@Nonnull final PlanSerializationContext serializationContext,
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

    /**
     * Helper method to check for a field's existence in the protobuf message passed in. If the field exists, the method
     * then extracts and returns the field's value. The method throws an {@link RecordCoreException} if the field does
     * not exist.
     * Note that this method is mostly equivalent to {@code Objects.requireNonNull(message.getSomeField())} if
     * {@code someField} does not use a default value, but also works for primitive values.
     * @param message protobuf message
     * @param fieldSetPredicate predicate to check if a field is set
     * @param fieldExtractor extractor extracting the field's value
     * @param <M> the specific message type
     * @param <T> the specific field's type
     * @return the field's value
     */
    @Nonnull
    public static <M extends Message, T> T getFieldOrThrow(@Nonnull M message,
                                                           @Nonnull final Predicate<M> fieldSetPredicate,
                                                           @Nonnull final Function<M, T> fieldExtractor) {
        if (fieldSetPredicate.test(message)) {
            return fieldExtractor.apply(message);
        }
        throw new RecordCoreException("Field is expected to be set but message does not have field.");
    }

    /**
     * Helper method to check for a field's existence in the protobuf message passed in. If the field exists, the method
     * then extracts and returns the field's value. The method returns {@code null} if the field does not exist.
     * Note that this method is mostly equivalent to {@code message.getSomeField()} if
     * {@code someField} does not use a default value, but also works for primitive values.
     * @param message protobuf message
     * @param fieldSetPredicate predicate to check if a field is set
     * @param fieldExtractor extractor extracting the field's value
     * @param <M> the specific message type
     * @param <T> the specific field's type
     * @return the field's value
     */
    @Nullable
    public static <M extends Message, T> T getFieldOrNull(@Nonnull M message,
                                                          @Nonnull final Predicate<M> fieldSetPredicate,
                                                          @Nonnull final Function<M, T> fieldExtractor) {
        if (fieldSetPredicate.test(message)) {
            return fieldExtractor.apply(message);
        }
        return null;
    }

    /**
     * {@link BiMap} to cache enum relationships. This helper has been added to avoid using huge switch
     * statements that relate domain enums and proto enums.
     * Note that both enum classes must use the same enum value names and ordinals.
     * @param domainEnumClass domain enum
     * @param protoEnumClass proto enum
     * @param <E1> domain enum type parameter
     * @param <E2> proto enum type parameter
     * @return a {@link BiMap} that can be used to look up the <em>other</em> enum value for a given enum value
     */
    @Nonnull
    public static <E1 extends Enum<E1>, E2 extends Enum<E2> & ProtocolMessageEnum> BiMap<E1, E2> protoEnumBiMap(@Nonnull final Class<E1> domainEnumClass,
                                                                                                                @Nonnull final Class<E2> protoEnumClass) {
        Verify.verify(domainEnumClass.isEnum());
        Verify.verify(protoEnumClass.isEnum());
        final E1[] javaEnumConstants = domainEnumClass.getEnumConstants();
        final E2[] protoEnumConstants = protoEnumClass.getEnumConstants();
        Verify.verify(javaEnumConstants.length == protoEnumConstants.length);

        final EnumBiMap<E1, E2> enumBiMap = EnumBiMap.create(domainEnumClass, protoEnumClass);
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
