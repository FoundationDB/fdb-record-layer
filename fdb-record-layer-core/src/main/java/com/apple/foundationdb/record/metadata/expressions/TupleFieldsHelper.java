/*
 * TupleFieldsHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.UUID;

/**
 * Static methods for dealing with special field types that are messages in protobuf but single items in {@link com.apple.foundationdb.tuple.Tuple}s.
 */
@API(API.Status.INTERNAL)
public class TupleFieldsHelper {
    @Nonnull
    private static final Set<Descriptors.Descriptor> DESCRIPTORS = ImmutableSet.of(
            TupleFieldsProto.UUID.getDescriptor(),
            TupleFieldsProto.NullableDouble.getDescriptor(),
            TupleFieldsProto.NullableFloat.getDescriptor(),
            TupleFieldsProto.NullableInt32.getDescriptor(),
            TupleFieldsProto.NullableInt64.getDescriptor(),
            TupleFieldsProto.NullableBool.getDescriptor(),
            TupleFieldsProto.NullableString.getDescriptor(),
            TupleFieldsProto.NullableBytes.getDescriptor()
    );

    /**
     * Test whether a message field's descriptor is one of the special ones.
     * @param descriptor message descriptor for the field
     * @return {@code true} if the field is decoded into a single item in a {@code Tuple}.
     */
    public static boolean isTupleField(@Nonnull Descriptors.Descriptor descriptor) {
        return DESCRIPTORS.contains(descriptor);
    }

    /**
     * Convert a field's value from a message to a {@link com.apple.foundationdb.tuple.Tuple} item.
     * @param value value for the field
     * @param descriptor message descriptor for the field
     * @return an object suitable for storing in a {@code Tuple}
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static Object fromProto(@Nonnull Message value, @Nonnull Descriptors.Descriptor descriptor) {
        if (descriptor == TupleFieldsProto.UUID.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.UUID ? (TupleFieldsProto.UUID)value : TupleFieldsProto.UUID.newBuilder().mergeFrom(value).build());
        } else if (descriptor == TupleFieldsProto.NullableDouble.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.NullableDouble ? (TupleFieldsProto.NullableDouble)value : TupleFieldsProto.NullableDouble.newBuilder().mergeFrom(value).build());
        } else if (descriptor == TupleFieldsProto.NullableFloat.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.NullableFloat ? (TupleFieldsProto.NullableFloat)value : TupleFieldsProto.NullableFloat.newBuilder().mergeFrom(value).build());
        } else if (descriptor == TupleFieldsProto.NullableInt32.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.NullableInt32 ? (TupleFieldsProto.NullableInt32)value : TupleFieldsProto.NullableInt32.newBuilder().mergeFrom(value).build());
        } else if (descriptor == TupleFieldsProto.NullableInt64.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.NullableInt64 ? (TupleFieldsProto.NullableInt64)value : TupleFieldsProto.NullableInt64.newBuilder().mergeFrom(value).build());
        } else if (descriptor == TupleFieldsProto.NullableBool.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.NullableBool ? (TupleFieldsProto.NullableBool)value : TupleFieldsProto.NullableBool.newBuilder().mergeFrom(value).build());
        } else if (descriptor == TupleFieldsProto.NullableString.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.NullableString ? (TupleFieldsProto.NullableString)value : TupleFieldsProto.NullableString.newBuilder().mergeFrom(value).build());
        } else if (descriptor == TupleFieldsProto.NullableBytes.getDescriptor()) {
            return fromProto(value instanceof TupleFieldsProto.NullableBytes ? (TupleFieldsProto.NullableBytes)value : TupleFieldsProto.NullableBytes.newBuilder().mergeFrom(value).build());
        } else {
            throw new RecordCoreArgumentException("value is not of a known message type");
        }
    }

    public static Descriptors.Descriptor getNullableWrapperDescriptorForTypeCode(Type.TypeCode typeCode) {
        switch (typeCode) {
            case INT:
                return TupleFieldsProto.NullableInt32.getDescriptor();
            case LONG:
                return TupleFieldsProto.NullableInt64.getDescriptor();
            case DOUBLE:
                return TupleFieldsProto.NullableDouble.getDescriptor();
            case FLOAT:
                return TupleFieldsProto.NullableFloat.getDescriptor();
            case BYTES:
                return TupleFieldsProto.NullableBytes.getDescriptor();
            case BOOLEAN:
                return TupleFieldsProto.NullableBool.getDescriptor();
            default:
                throw new SemanticException(SemanticException.ErrorCode.UNSUPPORTED, "nullable for type " + typeCode.name() + "is not supported (yet).", null);
        }
    }

    public static Type getTypeForNullableWrapper(@Nonnull Descriptors.Descriptor descriptor) {
        if (descriptor == TupleFieldsProto.UUID.getDescriptor()) {
            // just for simplicity, lets just say that nullable UUID do exist.
            return Type.uuidType(false);
        } else if (descriptor == TupleFieldsProto.NullableDouble.getDescriptor()) {
            return Type.primitiveType(Type.TypeCode.DOUBLE);
        } else if (descriptor == TupleFieldsProto.NullableFloat.getDescriptor()) {
            return Type.primitiveType(Type.TypeCode.FLOAT);
        } else if (descriptor == TupleFieldsProto.NullableInt32.getDescriptor()) {
            return Type.primitiveType(Type.TypeCode.INT);
        } else if (descriptor == TupleFieldsProto.NullableInt64.getDescriptor()) {
            return Type.primitiveType(Type.TypeCode.LONG);
        } else if (descriptor == TupleFieldsProto.NullableBool.getDescriptor()) {
            return Type.primitiveType(Type.TypeCode.BOOLEAN);
        } else if (descriptor == TupleFieldsProto.NullableString.getDescriptor()) {
            return Type.primitiveType(Type.TypeCode.STRING);
        } else if (descriptor == TupleFieldsProto.NullableBytes.getDescriptor()) {
            return Type.primitiveType(Type.TypeCode.BYTES);
        } else {
            throw new RecordCoreArgumentException("value is not of a known message type");
        }
    }

    /**
     * Convert a Protobuf {@code UUID} to a Java {@link UUID}.
     * @param proto the value of a Protobuf {@code UUID} field
     * @return the decoded Java {@code UUID}
     */
    @Nonnull
    public static UUID fromProto(@Nonnull TupleFieldsProto.UUID proto) {
        return new UUID(proto.getMostSignificantBits(), proto.getLeastSignificantBits());
    }

    /**
     * Convert a Protobuf {@code NullableDouble} to a Java {@code double}.
     * @param proto the value of a Protobuf {@code NullableDouble} field
     * @return the decoded Java {@code double}
     */
    public static double fromProto(@Nonnull TupleFieldsProto.NullableDouble proto) {
        return proto.getValue();
    }

    /**
     * Convert a Protobuf {@code NullableFloat} to a Java {@code float}.
     * @param proto the value of a Protobuf {@code NullableFloat} field
     * @return the decoded Java {@code float}
     */
    public static float fromProto(@Nonnull TupleFieldsProto.NullableFloat proto) {
        return proto.getValue();
    }

    /**
     * Convert a Protobuf {@code NullableInt32} to a Java {@code int}.
     * @param proto the value of a Protobuf {@code NullableInt32} field
     * @return the decoded Java {@code int}
     */
    public static int fromProto(@Nonnull TupleFieldsProto.NullableInt32 proto) {
        return proto.getValue();
    }

    /**
     * Convert a Protobuf {@code NullableInt64} to a Java {@code long}.
     * @param proto the value of a Protobuf {@code NullableInt64} field
     * @return the decoded Java {@code long}
     */
    public static long fromProto(@Nonnull TupleFieldsProto.NullableInt64 proto) {
        return proto.getValue();
    }

    /**
     * Convert a Protobuf {@code NullableBool} to a Java {@code boolean}.
     * @param proto the value of a Protobuf {@code NullableBool} field
     * @return the decoded Java {@code boolean}
     */
    public static boolean fromProto(@Nonnull TupleFieldsProto.NullableBool proto) {
        return proto.getValue();
    }

    /**
     * Convert a Protobuf {@code NullableString} to a Java {@link String}.
     * @param proto the value of a Protobuf {@code NullableString} field
     * @return the decoded Java {@code String}
     */
    @Nonnull
    public static String fromProto(@Nonnull TupleFieldsProto.NullableString proto) {
        return proto.getValue();
    }

    /**
     * Convert a Protobuf {@code NullableBytes} to a Java {@link ByteString}.
     * @param proto the value of a Protobuf {@code NullableBytes} field
     * @return the decoded Java {@code ByteString}
     */
    @Nonnull
    public static ByteString fromProto(@Nonnull TupleFieldsProto.NullableBytes proto) {
        return proto.getValue();
    }

    /**
     * Convert a field's value from a {@link com.apple.foundationdb.tuple.Tuple} item to a message.
     * @param value value for the field
     * @param descriptor message descriptor for the field
     * @return an object suitable for storing in the field
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static Message toProto(@Nonnull Object value, @Nonnull final Descriptors.Descriptor descriptor) {
        if (descriptor == TupleFieldsProto.UUID.getDescriptor()) {
            return toProto((UUID)value);
        } else if (descriptor == TupleFieldsProto.NullableDouble.getDescriptor()) {
            return toProto((Double)value);
        } else if (descriptor == TupleFieldsProto.NullableFloat.getDescriptor()) {
            return toProto((Float)value);
        } else if (descriptor == TupleFieldsProto.NullableInt32.getDescriptor()) {
            return toProto((Integer)value);
        } else if (descriptor == TupleFieldsProto.NullableInt64.getDescriptor()) {
            return toProto((Long)value);
        } else if (descriptor == TupleFieldsProto.NullableBool.getDescriptor()) {
            return toProto((Boolean)value);
        } else if (descriptor == TupleFieldsProto.NullableString.getDescriptor()) {
            return toProto((String)value);
        } else if (descriptor == TupleFieldsProto.NullableBytes.getDescriptor()) {
            return toProto(value instanceof byte[] ? ZeroCopyByteString.wrap((byte[])value) : (ByteString)value);
        } else {
            throw new RecordCoreArgumentException("value is not of a known message type");
        }
    }

    /**
     * Convert a Java {@link UUID} to a Protobuf {@code UUID}.
     * @param uuid the Java {@code UUID}
     * @return a message to set as the value of a Protobuf {@code UUID} field
     */
    @Nonnull
    public static TupleFieldsProto.UUID toProto(@Nonnull UUID uuid) {
        return TupleFieldsProto.UUID.newBuilder()
                .setMostSignificantBits(uuid.getMostSignificantBits())
                .setLeastSignificantBits(uuid.getLeastSignificantBits())
                .build();
    }

    /**
     * Convert a Java {@code double} to a Protobuf {@code NullableDouble}.
     * @param value the Java {@code double}
     * @return a message to set as the value of a Protobuf {@code NullableDouble} field
     */
    @Nonnull
    public static TupleFieldsProto.NullableDouble toProto(double value) {
        return TupleFieldsProto.NullableDouble.newBuilder().setValue(value) .build();
    }

    /**
     * Convert a Java {@code float} to a Protobuf {@code NullableFloat}.
     * @param value the Java {@code float}
     * @return a message to set as the value of a Protobuf {@code NullableFloat} field
     */
    @Nonnull
    public static TupleFieldsProto.NullableFloat toProto(float value) {
        return TupleFieldsProto.NullableFloat.newBuilder().setValue(value) .build();
    }

    /**
     * Convert a Java {@code int} to a Protobuf {@code NullableInt32}.
     * @param value the Java {@code int}
     * @return a message to set as the value of a Protobuf {@code NullableInt32} field
     */
    @Nonnull
    public static TupleFieldsProto.NullableInt32 toProto(int value) {
        return TupleFieldsProto.NullableInt32.newBuilder().setValue(value) .build();
    }

    /**
     * Convert a Java {@code long} to a Protobuf {@code NullableInt64}.
     * @param value the Java {@code long}
     * @return a message to set as the value of a Protobuf {@code NullableInt64} field
     */
    @Nonnull
    public static TupleFieldsProto.NullableInt64 toProto(long value) {
        return TupleFieldsProto.NullableInt64.newBuilder().setValue(value) .build();
    }

    /**
     * Convert a Java {@code boolean} to a Protobuf {@code NullableBool}.
     * @param value the Java {@code boolean}
     * @return a message to set as the value of a Protobuf {@code NullableBool} field
     */
    @Nonnull
    public static TupleFieldsProto.NullableBool toProto(boolean value) {
        return TupleFieldsProto.NullableBool.newBuilder().setValue(value) .build();
    }

    /**
     * Convert a Java {@link String} to a Protobuf {@code NullableString}.
     * @param value the Java {@code String}
     * @return a message to set as the value of a Protobuf {@code NullableString} field
     */
    @Nonnull
    public static TupleFieldsProto.NullableString toProto(@Nonnull String value) {
        return TupleFieldsProto.NullableString.newBuilder().setValue(value) .build();
    }

    /**
     * Convert a Java {@link ByteString} to a Protobuf {@code NullableBytes}.
     * @param value the Java {@code ByteString}
     * @return a message to set as the value of a Protobuf {@code NullableBytes} field
     */
    @Nonnull
    public static TupleFieldsProto.NullableBytes toProto(@Nonnull ByteString value) {
        return TupleFieldsProto.NullableBytes.newBuilder().setValue(value) .build();
    }

    private TupleFieldsHelper() {
    }
}
