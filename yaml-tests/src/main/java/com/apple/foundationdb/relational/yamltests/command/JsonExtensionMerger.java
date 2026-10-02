/*
 * JsonExtensionMerger.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-attach the proto2 extensions that {@link JsonFormat} discards while parsing metadata from JSON.
 * <p>
 * {@code JsonFormat} implements the proto3 JSON mapping, which has no notion of extensions: they are treated as if they
 * did not exist. Extensions do, however, carry information the metadata cannot do without — a vector field is a
 * {@code bytes} field whose precision and dimensions live in an extension of {@code google.protobuf.FieldOptions}, so
 * dropping it silently turns the field into a plain {@code bytes} field. So this walks the JSON alongside the builder it
 * was parsed into and sets every extension the JSON names:
 * <pre>{@code
 * "embedding": { "type": "TYPE_BYTES",
 *                "options": { "com.apple.foundationdb.record.field": {
 *                             "vectorOptions": { "precision": 64, "dimensions": 512 }}}}
 * }</pre>
 * An extension must be named by its full name, as above; its declared name ({@code "field"}) is not accepted, since two
 * extensions of one message may share one and the short form would be ambiguous. Note that
 * {@link JsonFormat#printer()} emits the short form, so a file it writes does not round-trip its extensions through
 * this class.
 * </p>
 */
final class JsonExtensionMerger {
    /**
     * The magnitude a float extension may hold. {@code JsonFormat} allows a little beyond the range, since printing a
     * float and reading it back can come out very slightly larger, and this follows it.
     */
    private static final BigDecimal MAX_FLOAT = BigDecimal.valueOf(Float.MAX_VALUE * (1.0 + 1.0e-6));
    private static final BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);

    @Nonnull
    private final ExtensionRegistry registry;
    /** The fields of a message by the JSON keys naming them. Metadata of any size meets the same few messages over. */
    @Nonnull
    private final Map<Descriptors.Descriptor, Map<String, Descriptors.FieldDescriptor>> fieldsByType = new HashMap<>();

    private JsonExtensionMerger(@Nonnull ExtensionRegistry registry) {
        this.registry = registry;
    }

    /**
     * Set on {@code builder} every extension that {@code json} names, at any depth.
     *
     * @param builder the builder the {@code json} object was parsed into
     * @param json the JSON object the builder was parsed from
     * @param dependencyClasses the generated outer classes of the resolved dependencies, which declare the extensions
     * the {@code json} may name beyond those of the metadata protos themselves
     */
    static void merge(@Nonnull Message.Builder builder,
                      @Nonnull JsonObject json,
                      @Nonnull List<Class<?>> dependencyClasses) {
        new JsonExtensionMerger(registryFor(dependencyClasses)).mergeExtensions(builder, json);
    }

    @Nonnull
    private static ExtensionRegistry registryFor(@Nonnull List<Class<?>> dependencyClasses) {
        final ExtensionRegistry registry = ExtensionRegistry.newInstance();
        for (Class<?> dependencyClass : dependencyClasses) {
            try {
                // every generated outer class declares this, whether or not its file has extensions to register
                dependencyClass.getMethod("registerAllExtensions", ExtensionRegistry.class).invoke(null, registry);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("unable to register the extensions of " + dependencyClass.getName(), e);
            }
        }
        // Last, and deliberately: the registry is keyed by (extended message, field number) and a later add wins, so
        // registering the record layer's own extensions after the dependencies keeps a dependency that claims one of
        // their numbers from evicting them.
        RecordMetaDataOptionsProto.registerAllExtensions(registry);
        return registry;
    }

    private void mergeExtensions(@Nonnull Message.Builder builder, @Nonnull JsonObject json) {
        final Descriptors.Descriptor descriptor = builder.getDescriptorForType();
        final Map<String, Descriptors.FieldDescriptor> fields = fieldsOf(descriptor);
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            // Resolve as a regular field first: that is what JsonFormat did with the key, and it keeps the everyday
            // keys of the metadata from reaching the registry at all.
            final Descriptors.FieldDescriptor field = fields.get(entry.getKey());
            if (field == null) {
                mergeExtension(builder, descriptor, entry.getKey(), entry.getValue());
            } else {
                mergeFieldExtensions(builder, field, entry.getValue());
            }
        }
    }

    private void mergeExtension(@Nonnull Message.Builder builder,
                                @Nonnull Descriptors.Descriptor descriptor,
                                @Nonnull String key,
                                @Nonnull JsonElement value) {
        final ExtensionRegistry.ExtensionInfo extension = registry.findImmutableExtensionByName(key);
        if (extension != null && descriptor.equals(extension.descriptor.getContainingType())) {
            setExtension(builder, extension.descriptor, extension.defaultInstance, value);
        } else if (key.indexOf('.') >= 0) {
            // A field name cannot contain a dot, so a dotted key is an extension — one no resolved dependency
            // declares. Leaving it be would drop it silently, which is the failure this class exists to prevent.
            throw new IllegalArgumentException("no registered extension named " + key + " of "
                    + descriptor.getFullName());
        }
        // An undotted key that names neither a field of this build nor a registered extension is one of two things,
        // indistinguishable here, so both are left as the parse left them: a field of a newer version of these protos,
        // which the parse discarded and which must not be an error if an older build is to read a newer file; or an
        // extension spelled by its declared name, the form JsonFormat's printer writes, which is not unique among the
        // extensions of one message and so cannot be resolved.
    }

    /**
     * Descend into a field that may hold extensions further down. Shapes are checked before the builder is touched:
     * {@code JsonFormat} accepts {@code null} for any field, and {@link Message.Builder#getFieldBuilder} is a mutator
     * that would mark an absent field present.
     */
    private void mergeFieldExtensions(@Nonnull Message.Builder builder,
                                      @Nonnull Descriptors.FieldDescriptor field,
                                      @Nonnull JsonElement value) {
        if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE || field.isMapField()) {
            // A scalar holds no extensions. Neither does a map entry, which is a repeated message to the descriptor
            // but a JSON object here, and has no nested builder to walk.
            return;
        }
        if (!field.isRepeated()) {
            if (value.isJsonObject()) {
                mergeExtensions(builder.getFieldBuilder(field), value.getAsJsonObject());
            }
            return;
        }
        if (value.isJsonArray()) {
            final JsonArray elements = value.getAsJsonArray();
            // The builder was parsed from this very array, by a parser that reads the JSON with Gson as this class does
            // and refuses a null element, so the two hold the same elements in the same order.
            for (int i = 0; i < elements.size(); i++) {
                mergeExtensions(builder.getRepeatedFieldBuilder(field, i), elements.get(i).getAsJsonObject());
            }
        }
    }

    private void setExtension(@Nonnull Message.Builder builder,
                              @Nonnull Descriptors.FieldDescriptor extension,
                              @Nullable Message defaultInstance,
                              @Nonnull JsonElement value) {
        if (extension.isRepeated()) {
            throw new UnsupportedOperationException("repeated extension is not supported: "
                    + extension.getFullName());
        }
        builder.setField(extension, extensionValue(extension, defaultInstance, value));
    }

    /**
     * Convert the JSON form of an extension value to what {@link Message.Builder#setField} expects. The conversions
     * follow the proto3 JSON mapping as {@code JsonFormat} implements it for an ordinary field of the same type, since
     * the same file spells its fields and its extensions the same way.
     */
    @Nonnull
    private Object extensionValue(@Nonnull Descriptors.FieldDescriptor extension,
                                  @Nullable Message defaultInstance,
                                  @Nonnull JsonElement value) {
        return switch (extension.getJavaType()) {
            case MESSAGE -> {
                // ExtensionInfo carries a default instance for exactly the message-valued extensions
                if (defaultInstance == null) {
                    throw new IllegalStateException("message extension registered without a default instance: "
                            + extension.getFullName());
                }
                yield messageValue(extension, defaultInstance, value);
            }
            case INT -> (int) integerValue(extension, value, Integer.SIZE);
            case LONG -> integerValue(extension, value, Long.SIZE);
            case FLOAT -> (float) realValue(extension, value, MAX_FLOAT, "float");
            case DOUBLE -> realValue(extension, value, MAX_DOUBLE, "double");
            case STRING -> value.getAsString();
            case BOOLEAN -> {
                // JsonFormat's parseBool compares the string form of the value, so it takes the bare literal and its
                // quoted spelling alike and nothing else. Coercing instead, as Gson's getAsBoolean does, lands every
                // unrecognised value on false, which is also the default of a bool extension and so leaves no trace.
                if (!value.isJsonPrimitive()) {
                    throw new IllegalArgumentException(notA("boolean", extension, value));
                }
                yield switch (value.getAsString()) {
                        case "true" -> true;
                        case "false" -> false;
                        default -> throw new IllegalArgumentException(notA("boolean", extension, value));
                    };
            }
            case BYTE_STRING -> {
                // the printer emits the standard alphabet, but JsonFormat reads either
                final String encoded = value.getAsString();
                try {
                    yield ByteString.copyFrom(Base64.getDecoder().decode(encoded));
                } catch (IllegalArgumentException standardAlphabet) {
                    yield ByteString.copyFrom(Base64.getUrlDecoder().decode(encoded));
                }
            }
            case ENUM -> {
                // a name, as the printer writes it, or the number, which JsonFormat also accepts
                final Descriptors.EnumDescriptor type = extension.getEnumType();
                final Descriptors.EnumValueDescriptor enumValue =
                        value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()
                        ? type.findValueByName(value.getAsString())
                        : type.findValueByNumber(value.getAsInt());
                if (enumValue == null) {
                    throw new IllegalArgumentException(notA("value of " + type.getFullName(), extension, value));
                }
                yield enumValue;
            }
        };
    }

    @Nonnull
    private Message messageValue(@Nonnull Descriptors.FieldDescriptor extension,
                                 @Nonnull Message defaultInstance,
                                 @Nonnull JsonElement value) {
        final Message.Builder valueBuilder = defaultInstance.newBuilderForType();
        try {
            // Unknown fields are tolerated because an extension of this extension is, by definition, a field
            // JsonFormat knows nothing about; the walk below is what picks those up.
            JsonFormat.parser().ignoringUnknownFields().merge(value.toString(), valueBuilder);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("unable to parse value of extension " + extension.getFullName(), e);
        }
        if (value.isJsonObject()) {
            // not an object for a message with its own JSON form, such as a Timestamp
            mergeExtensions(valueBuilder, value.getAsJsonObject());
        }
        return valueBuilder.build();
    }

    /**
     * Read a whole number that fits the signed range of {@code bits} bits. Unsigned types are not accommodated: they
     * are unsupported above this class, and their JSON form is the unsigned decimal value, which does not fit the signed
     * Java type protobuf holds them in.
     */
    private static long integerValue(@Nonnull Descriptors.FieldDescriptor extension,
                                     @Nonnull JsonElement value,
                                     int bits) {
        final BigInteger number;
        try {
            number = new BigDecimal(value.getAsString()).toBigIntegerExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(notA("whole number", extension, value), e);
        }
        if (number.bitLength() >= bits) {
            throw new IllegalArgumentException(notA("signed " + bits + " bit number", extension, value));
        }
        return number.longValue();
    }

    /**
     * Read a real number whose magnitude {@code limit} allows. The range check is the point of the method:
     * {@code JsonFormat} makes the same one, and says why it does not simply call {@code Float.parseFloat}, which takes
     * every literal and answers an infinity for one out of range — a value the extension cannot hold arriving as one it
     * can, with nothing to show that it happened.
     */
    private static double realValue(@Nonnull Descriptors.FieldDescriptor extension,
                                    @Nonnull JsonElement value,
                                    @Nonnull BigDecimal limit,
                                    @Nonnull String type) {
        final Double nonFinite = nonFiniteValue(value);
        if (nonFinite != null) {
            return nonFinite;
        }
        if (!value.isJsonPrimitive()) {
            throw new IllegalArgumentException(notA(type, extension, value));
        }
        final BigDecimal number;
        try {
            number = new BigDecimal(value.getAsString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(notA(type, extension, value), e);
        }
        if (number.abs().compareTo(limit) > 0) {
            throw new IllegalArgumentException(notA(type + " in range", extension, value));
        }
        return number.doubleValue();
    }

    /**
     * The three values the proto3 JSON mapping spells as strings rather than as numbers, or {@code null} where the
     * value is not one of them. They are the only spellings {@code JsonFormat} takes for them.
     */
    @Nullable
    private static Double nonFiniteValue(@Nonnull JsonElement value) {
        if (!value.isJsonPrimitive()) {
            return null;
        }
        return switch (value.getAsString()) {
            case "NaN" -> Double.NaN;
            case "Infinity" -> Double.POSITIVE_INFINITY;
            case "-Infinity" -> Double.NEGATIVE_INFINITY;
            default -> null;
        };
    }

    @Nonnull
    private static String notA(@Nonnull String expected,
                               @Nonnull Descriptors.FieldDescriptor extension,
                               @Nonnull JsonElement value) {
        return "value of extension " + extension.getFullName() + " is not a " + expected + ": " + value;
    }

    /**
     * Index the fields of a message by the keys the proto3 JSON mapping allows for them, which are the name a field is
     * declared with and its lower camel case form. This mirrors {@code JsonFormat.ParserImpl.getFieldNameMap}, so the
     * field that won during the parse wins here too and the walk cannot descend into a builder the value was not
     * parsed into.
     */
    @Nonnull
    private Map<String, Descriptors.FieldDescriptor> fieldsOf(@Nonnull Descriptors.Descriptor descriptor) {
        return fieldsByType.computeIfAbsent(descriptor, ignored -> {
            final Map<String, Descriptors.FieldDescriptor> fields = new HashMap<>();
            for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
                fields.put(field.getName(), field);
                fields.put(field.getJsonName(), field);
            }
            return fields;
        });
    }
}
