/*
 * MessageHelpers.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.TrieNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A utility class for extracting data and meta-data from Protocol Buffer {@link Message}s, as used in the Record Layer.
 */
@API(API.Status.EXPERIMENTAL)
public class MessageHelpers {
    private MessageHelpers() {
        // shut up compiler
    }

    /**
     * Get the value of the (nested) field on the path from the message defined by {@code fieldNames}.
     * The given field names define a path through the nested structure of the given message; this method traverses
     * that path and returns the value at the leaf, using the return semantics of {@link #getFieldOnMessage(MessageOrBuilder, String)}.
     *
     * @param message a message
     * @param fieldNames a list of field names defining a path starting at {@code message}
     * @return the value at the end of the path
     */
    @Nullable
    public static Object getFieldValueForFieldNames(@Nonnull MessageOrBuilder message, @Nonnull List<String> fieldNames) {
        if (fieldNames.isEmpty()) {
            throw new RecordCoreException("empty list of field names");
        }
        MessageOrBuilder current = message;
        int fieldNamesIndex;
        // Notice that up to fieldNames.size() - 2 are calling getFieldMessageOnMessage, and fieldNames.size() - 1 is calling getFieldOnMessage
        for (fieldNamesIndex = 0; fieldNamesIndex < fieldNames.size() - 1; fieldNamesIndex++) {
            current = getFieldMessageOnMessage(current, fieldNames.get(fieldNamesIndex));
            if (current == null) {
                return null;
            }
        }
        return getFieldOnMessage(current, fieldNames.get(fieldNames.size() - 1));
    }

    /**
     * Get the value of the (nested) field on the path from the message defined by {@code fieldNames}.
     * The given field names define a path through the nested structure of the given message; this method traverses
     * that path and returns the value at the leaf, using the return semantics of {@link #getFieldOnMessage(MessageOrBuilder, String)}.
     *
     * @param message a message
     * @param fields a list of field defining a path starting at {@code message}
     * @return the value at the end of the path
     */
    @Nullable
    public static Object getFieldValueForFields(@Nonnull MessageOrBuilder message, @Nonnull List<Type.Record.Field> fields) {
        if (fields.isEmpty()) {
            throw new RecordCoreException("empty list of fields");
        }
        MessageOrBuilder current = message;
        int fieldNamesIndex;
        // Notice that up to fieldNames.size() - 2 are calling getFieldMessageOnMessage, and fieldNames.size() - 1 is calling getFieldOnMessage
        for (fieldNamesIndex = 0; fieldNamesIndex < fields.size() - 1; fieldNamesIndex++) {
            current = getFieldMessageOnMessage(current, fields.get(fieldNamesIndex).getFieldIndex());
            if (current == null) {
                return null;
            }
        }
        return getFieldOnMessage(current, fields.get(fields.size() - 1).getFieldIndex());
    }

    @SuppressWarnings("UnstableApiUsage") // caused by usage of Guava's ImmutableIntArray.
    @Nullable
    public static Object getFieldValueForFieldOrdinals(@Nonnull MessageOrBuilder message, @Nonnull ImmutableIntArray fieldOrdinals) {
        if (fieldOrdinals.isEmpty()) {
            throw new RecordCoreException("empty list of fields");
        }
        MessageOrBuilder current = message;
        int fieldOrdinal;
        // Notice that up to fieldOrdinals.length() - 2 are calling getFieldMessageOnMessageByOrdinal, and fieldOrdinals.length() - 1 is calling getFieldOnMessageByOrdinal
        for (fieldOrdinal = 0; fieldOrdinal < fieldOrdinals.length() - 1; fieldOrdinal++) {
            current = getFieldMessageOnMessageByOrdinal(current, fieldOrdinals.get(fieldOrdinal));
            if (current == null) {
                return null;
            }
        }
        return getFieldOnMessageByOrdinal(current, fieldOrdinals.get(fieldOrdinals.length() - 1));
    }

    /**
     * Get the value of the field with the given field name on the given message.
     * If the field is repeated, the repeated values are combined into a list. If the field has a message type,
     * the value is returned as a {@link Message} of that type. Otherwise, the field is returned as a primitive.
     * @param message a message or builder to extract the field from
     * @param fieldName the field to extract
     * @return the value of the field as described above
     */
    @Nullable
    public static Object getFieldOnMessage(@Nonnull MessageOrBuilder message, @Nonnull String fieldName) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessage(message, fieldName);
        return getFieldOnMessage(message, field);
    }

    /**
     * Get the value of the field with the given field name on the given message.
     * If the field is repeated, the repeated values are combined into a list. If the field has a message type,
     * the value is returned as a {@link Message} of that type. Otherwise, the field is returned as a primitive.
     * @param message a message or builder to extract the field from
     * @param fieldIndex the field number to extract
     * @return the value of the field as described above
     */
    @Nullable
    public static Object getFieldOnMessage(@Nonnull MessageOrBuilder message, int fieldIndex) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessage(message, fieldIndex);
        return getFieldOnMessage(message, field);
    }

    @Nullable
    public static Object getFieldOnMessage(@Nonnull MessageOrBuilder message, @Nonnull Descriptors.FieldDescriptor field) {
        if (field.isRepeated()) {
            int count = message.getRepeatedFieldCount(field);
            List<Object> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                list.add(message.getRepeatedField(field, i));
            }
            return list;
        }
        if (field.hasDefaultValue() || message.hasField(field)) {
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE &&
                    TupleFieldsHelper.isTupleField(field.getMessageType())) {
                return TupleFieldsHelper.fromProto((Message)message.getField(field), field.getMessageType());
            } else {
                return message.getField(field);
            }
        } else {
            return null;
        }
    }

    @Nullable
    public static Object getFieldOnMessageByOrdinal(@Nonnull MessageOrBuilder message, int fieldOrdinal) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessageByOrdinal(message, fieldOrdinal);
        return getFieldOnMessage(message, field);
    }

    @Nonnull
    public static Descriptors.FieldDescriptor findFieldDescriptorOnMessage(@Nonnull MessageOrBuilder message, @Nonnull String fieldName) {
        final Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldName);
        }
        return field;
    }

    @Nonnull
    public static Descriptors.FieldDescriptor findFieldDescriptorOnMessage(@Nonnull MessageOrBuilder message, int fieldNumber) {
        final Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByNumber(fieldNumber);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldNumber);
        }
        return field;
    }

    @Nonnull
    public static Descriptors.FieldDescriptor findFieldDescriptorOnMessageByOrdinal(@Nonnull MessageOrBuilder message, int fieldOrdinal) {
        if (fieldOrdinal < 0 || fieldOrdinal >= message.getDescriptorForType().getFields().size()) {
            throw new Query.InvalidExpressionException("Missing field (#ord=" + fieldOrdinal + ")");
        }
        return message.getDescriptorForType().getFields().get(fieldOrdinal);
    }

    @Nullable
    private static Message getFieldMessageOnMessage(@Nonnull MessageOrBuilder message, @Nonnull String fieldName) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessage(message, fieldName);
        return getFieldMessageOnMessage(message, field);
    }

    @Nullable
    private static Message getFieldMessageOnMessage(@Nonnull MessageOrBuilder message, int fieldIndex) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessage(message, fieldIndex);
        return getFieldMessageOnMessage(message, field);
    }

    @Nullable
    private static Message getFieldMessageOnMessage(@Nonnull MessageOrBuilder message, final Descriptors.FieldDescriptor field) {
        if (!field.isRepeated() &&
                (field.hasDefaultValue() || message.hasField(field)) &&
                field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return (Message)message.getField(field);
        }
        return null;
    }

    @Nullable
    private static Message getFieldMessageOnMessageByOrdinal(@Nonnull MessageOrBuilder message, int fieldOrdinal) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessageByOrdinal(message, fieldOrdinal);
        return getFieldMessageOnMessage(message, field);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <M extends Message> Object transformMessage(@Nonnull final FDBRecordStoreBase<M> store,
                                                              @Nonnull final EvaluationContext context,
                                                              @Nullable final TransformationTrieNode transformationsTrie,
                                                              @Nullable final CoercionTrieNode coercionsTrie,
                                                              @Nonnull final Type targetType,
                                                              @Nullable Descriptors.Descriptor targetDescriptor,
                                                              @Nonnull final Type currentType,
                                                              @Nullable final Object current) {
        final var value = transformationsTrie == null ? null : transformationsTrie.getValue();
        Verify.verify(value == null);

        targetDescriptor = Verify.verifyNotNull(targetDescriptor);
        final var targetDescriptorFields = targetDescriptor.getFields();
        final var targetRecordType = (Type.Record)targetType;
        final var currentRecordType = (Type.Record)currentType;

        final var transformationsChildrenMap = transformationsTrie == null ? null : transformationsTrie.getChildrenMap();
        final var coercionsChildrenMap = coercionsTrie == null ? null : coercionsTrie.getChildrenMap();
        final var subRecord = (M)Verify.verifyNotNull(current);

        final var resultMessageBuilder = DynamicMessage.newBuilder(targetDescriptor);
        final var messageDescriptor = subRecord.getDescriptorForType();
        for (final var messageFieldDescriptor : messageDescriptor.getFields()) {
            final var accessorForField = ResolvedAccessor.of(currentRecordType.getField(messageFieldDescriptor.getIndex()), messageDescriptor.getIndex());
            final var transformationTrieForField = transformationsChildrenMap == null ? null : transformationsChildrenMap.get(accessorForField);
            final var targetFieldDescriptor = targetDescriptorFields.get(messageFieldDescriptor.getIndex());
            final var targetDescriptorForField = targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ? targetFieldDescriptor.getMessageType() : null;
            final var promotionTrieForField = coercionsChildrenMap == null ? null : coercionsChildrenMap.get(accessorForField);
            if (transformationTrieForField != null) {
                final var targetFieldType = targetRecordType.getField(accessorForField.getOrdinal()).getFieldType();
                final var currentFieldType = currentRecordType.getField(accessorForField.getOrdinal()).getFieldType();
                Object fieldResult;
                if (transformationTrieForField.getValue() == null) {
                    fieldResult =
                            transformMessage(store,
                                    context,
                                    transformationTrieForField,
                                    promotionTrieForField,
                                    targetFieldType,
                                    targetDescriptorForField,
                                    currentFieldType,
                                    subRecord.getField(messageFieldDescriptor));
                } else {
                    final var transformationValue = transformationTrieForField.getValue();
                    fieldResult = coerceObject(promotionTrieForField,
                            targetFieldType,
                            targetDescriptorForField,
                            currentFieldType,
                            transformationValue.eval(store, context));
                }
                Verify.verify(fieldResult != null || (currentFieldType.isNullable() && targetFieldType.isNullable()));
                if (fieldResult != null) {
                    resultMessageBuilder.setField(targetFieldDescriptor, fieldResult);
                }
            } else {
                var fieldResult = getFieldOnMessage(subRecord, messageFieldDescriptor);
                if (fieldResult != null) {
                    final var targetFieldType = Verify.verifyNotNull(targetRecordType.getField(messageFieldDescriptor.getIndex())).getFieldType();
                    final var currentFieldType = Verify.verifyNotNull(currentRecordType.getField(messageFieldDescriptor.getIndex())).getFieldType();
                    fieldResult = Verify.verifyNotNull(NullableArrayTypeUtils.unwrapIfArray(fieldResult, currentFieldType));
                    // coercedObject can only be NULL if fieldResult was NULL which cannot happen
                    final var coercedObject =
                            Verify.verifyNotNull(
                                    coerceObject(promotionTrieForField, targetFieldType, targetDescriptorForField, currentFieldType, fieldResult));
                    resultMessageBuilder.setField(targetFieldDescriptor, coercedObject);
                }
            }
        }
        return resultMessageBuilder.build();
    }

    /**
     * Coerce an object to become an object of a different type.
     * @param coercionsTrie a trie describing actual primitive promotions/casts that need to be carried out
     * @param targetType the target type this method coerces the object into
     * @param targetDescriptor the protobuf descriptor to be used to create the result message if {@code current} is a
     *        message (record, array)
     * @param currentType the source (current) type of the object passed in as {@code current}
     * @param current the <em>current</em> object
     * @return the coerced object (maybe {@code null})
     */
    @Nullable
    public static Object coerceObject(@Nullable final CoercionTrieNode coercionsTrie,
                                      @Nonnull final Type targetType,
                                      @Nullable final Descriptors.Descriptor targetDescriptor,
                                      @Nonnull final Type currentType,
                                      @Nullable final Object current) {
        //
        // This verify() is a not a semantic check, the condition we check for here should be impossible to reach, thus,
        // if still reached, it's not a user error.
        //
        Verify.verify(current != null || targetType.isNullable());

        // In any case, a NULL returns a NULL
        if (current == null) {
            return null;
        }

        //
        // This is the leaf case: If the target is primitive return the application of the coercion function to current
        // if the function exists; otherwise just return the object;
        //
        if (targetType.isPrimitive()) {
            if (coercionsTrie == null) {
                return current;
            }

            final var coercionFunction = Verify.verifyNotNull(coercionsTrie.getValue());
            return Verify.verifyNotNull(coercionFunction.apply(current));
        }

        //
        // This juggles with a change in nullability for arrays. If we were nullable before, but now we are not or
        // vice versa, we need to change the wrapping in protobuf.
        //
        if (currentType.getTypeCode() == Type.TypeCode.ARRAY) {
            Verify.verify(targetType.getTypeCode() == Type.TypeCode.ARRAY);
            final var targetElementType = Verify.verifyNotNull(((Type.Array)targetType).getElementType());
            final var currentElementType = Verify.verifyNotNull(((Type.Array)currentType).getElementType());

            final var currentObjects = (List<?>)current;
            final var coercedObjectsBuilder = ImmutableList.builder();
            for (final var currentObject : currentObjects) {
                // NULL as elements of a collection are currently not supported
                SemanticException.check(currentObject != null, SemanticException.ErrorCode.UNSUPPORTED);
                final var coercedObject =
                        Verify.verifyNotNull(coerceObject(coercionsTrie, targetElementType, targetDescriptor, currentElementType, currentObject));
                coercedObjectsBuilder.add(coercedObject);
            }
            final var coercedArray = coercedObjectsBuilder.build();

            if (currentType.isNullable()) {
                // the target descriptor is the wrapping holder
                final var verifiedTargetDescriptor = Verify.verifyNotNull(targetDescriptor);
                final var wrapperBuilder = DynamicMessage.newBuilder(verifiedTargetDescriptor);
                wrapperBuilder.setField(verifiedTargetDescriptor.findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()), coercedArray);
                return wrapperBuilder.build();
            } else {
                return coercedArray;
            }
        }

        if (targetType.getTypeCode() == Type.TypeCode.RECORD) {
            return coerceMessage(coercionsTrie, targetType, Verify.verifyNotNull(targetDescriptor), currentType, (Message)current);
        }

        throw new IllegalStateException("unsupported java type for record field");
    }

    @Nonnull
    public static Message coerceMessage(@Nullable final CoercionTrieNode coercionsTrie,
                                        @Nonnull final Type targetType,
                                        @Nonnull Descriptors.Descriptor targetDescriptor,
                                        @Nonnull final Type currentType,
                                        @Nonnull final Message currentMessage) {
        targetDescriptor = Verify.verifyNotNull(targetDescriptor);
        final var promotionsChildrenMap = coercionsTrie == null ? null : coercionsTrie.getChildrenMap();
        final var targetRecordType = (Type.Record)targetType;
        final var currentRecordType = (Type.Record)currentType;
        final var resultMessageBuilder = DynamicMessage.newBuilder(targetDescriptor);
        final var messageDescriptor = currentMessage.getDescriptorForType();
        final var targetFieldsFromDescriptor = targetDescriptor.getFields();
        for (final var messageFieldDescriptor : messageDescriptor.getFields()) {
            if (currentMessage.hasField(messageFieldDescriptor)) {
                final var targetFieldDescriptor = Verify.verifyNotNull(targetFieldsFromDescriptor.get(messageFieldDescriptor.getIndex()));
                final var targetFieldType = Verify.verifyNotNull(targetRecordType.getField(messageFieldDescriptor.getIndex())).getFieldType();
                final var currentField = currentRecordType.getField(messageFieldDescriptor.getIndex());
                final var accessorForCurrentField = ResolvedAccessor.of(currentField, messageFieldDescriptor.getIndex());
                final var currentFieldType = Verify.verifyNotNull(currentField).getFieldType();

                // coerced object can only be NULL if passed-in object is NULL which cannot happen here
                final var coercedObject =
                        Verify.verifyNotNull(
                                coerceObject(
                                        promotionsChildrenMap == null ? null : promotionsChildrenMap.get(accessorForCurrentField),
                                        targetFieldType,
                                        targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ? targetFieldDescriptor.getMessageType() : null,
                                        currentFieldType,
                                        currentMessage.getField(messageFieldDescriptor)));
                resultMessageBuilder.setField(targetFieldDescriptor, coercedObject);
            }
        }
        return resultMessageBuilder.build();
    }

    /**
     * Helper class to hold information about a particular field access.
     */
    public static class Accessor {
        @Nullable
        final String name;

        final int ordinal;

        public Accessor(@Nullable final String name, final int ordinal) {
            this.name = name;
            this.ordinal = ordinal;
        }

        @Nullable
        public String getName() {
            return name;
        }

        public int getOrdinal() {
            return ordinal;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Accessor)) {
                return false;
            }
            final Accessor accessor = (Accessor)o;
            return ordinal == accessor.ordinal && Objects.equals(name, accessor.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, ordinal);
        }
    }

    /**
     * A resolved {@link Accessor} that now also holds the resolved {@link Type}.
     */
    public static class ResolvedAccessor {
        @Nullable
        final String name;

        final int ordinal;

        @Nonnull
        private final Type type;

        private ResolvedAccessor(@Nullable final String name, final int ordinal, @Nonnull final Type type) {
            this.name = name;
            this.ordinal = ordinal;
            this.type = type;
        }

        @Nullable
        public String getName() {
            return name;
        }

        public int getOrdinal() {
            return ordinal;
        }

        @Nonnull
        public Type getType() {
            return type;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ResolvedAccessor)) {
                return false;
            }
            final ResolvedAccessor that = (ResolvedAccessor)o;
            return getOrdinal() == that.getOrdinal() &&
                   getType().equals(that.getType());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getOrdinal(), getType());
        }

        @Nonnull
        public static ResolvedAccessor of(@Nonnull final Type.Record.Field field, final int ordinal) {
            return of(field.getFieldNameOptional().orElse(null), ordinal, field.getFieldType());
        }

        @Nonnull
        public static ResolvedAccessor of(@Nullable final String fieldName, final int ordinalFieldNumber, @Nonnull final Type type) {
            Preconditions.checkArgument(ordinalFieldNumber >= 0);
            return new ResolvedAccessor(fieldName, ordinalFieldNumber, type);
        }
    }

    /**
     * Trie data structure of {@link Type.Record.Field}s to {@link Value}s.
     */
    public static class TransformationTrieNode extends TrieNode<ResolvedAccessor, Value, TransformationTrieNode> {
        /**
         * Map to track fieldName -> field associations in order to find transformations by name quicker.
         */
        @Nullable
        private final Map<Integer, ResolvedAccessor> ordinalToAccessorMap;

        public TransformationTrieNode(@Nullable final Value value, @Nullable final Map<ResolvedAccessor, TransformationTrieNode> childrenMap) {
            super(value, childrenMap);
            this.ordinalToAccessorMap = childrenMap == null ? null : computeFieldNameToFieldMap(childrenMap);
        }

        @Nullable
        public Map<Integer, ResolvedAccessor> getOrdinalToAccessorMap() {
            return ordinalToAccessorMap;
        }

        @Nonnull
        @Override
        public TransformationTrieNode getThis() {
            return this;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TransformationTrieNode)) {
                return false;
            }
            final TransformationTrieNode transformationTrieNode = (TransformationTrieNode)o;
            return Objects.equals(getValue(), transformationTrieNode.getValue()) &&
                   Objects.equals(getChildrenMap(), transformationTrieNode.getChildrenMap()) &&
                   Objects.equals(getOrdinalToAccessorMap(), transformationTrieNode.getOrdinalToAccessorMap());
        }

        public boolean semanticEquals(final Object other, @Nonnull final AliasMap equivalencesMap) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TransformationTrieNode)) {
                return false;
            }
            final TransformationTrieNode otherTransformationTrieNode = (TransformationTrieNode)other;

            return equalsNullable(getValue(), otherTransformationTrieNode.getValue(), (t, o) -> t.semanticEquals(o, equivalencesMap)) &&
                   equalsNullable(getChildrenMap(), otherTransformationTrieNode.getChildrenMap(), (t, o) -> semanticEqualsForChildrenMap(t, o, equivalencesMap)) &&
                   Objects.equals(getOrdinalToAccessorMap(), otherTransformationTrieNode.getOrdinalToAccessorMap());
        }

        private static boolean semanticEqualsForChildrenMap(@Nonnull final Map<ResolvedAccessor, TransformationTrieNode> self,
                                                            @Nonnull final Map<ResolvedAccessor, TransformationTrieNode> other,
                                                            @Nonnull final AliasMap equivalencesMap) {
            if (self.size() != other.size()) {
                return false;
            }

            for (final var fieldPath : self.keySet()) {
                final var selfNestedTrie = self.get(fieldPath);
                final var otherNestedTrie = self.get(fieldPath);
                if (!selfNestedTrie.semanticEquals(otherNestedTrie, equivalencesMap)) {
                    return false;
                }
            }
            return true;
        }

        private static <T> boolean equalsNullable(@Nullable final T self,
                                                  @Nullable final T other,
                                                  @Nonnull final BiFunction<T, T, Boolean> nonNullableTest) {
            if (self == null && other == null) {
                return true;
            }
            if (self == null) {
                return false;
            }
            return nonNullableTest.apply(self, other);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getValue(), getChildrenMap(), getOrdinalToAccessorMap());
        }

        @Nonnull
        private static Map<Integer, ResolvedAccessor> computeFieldNameToFieldMap(@Nonnull final Map<ResolvedAccessor, TransformationTrieNode> childrenMap) {
            final var resultBuilder = ImmutableMap.<Integer, ResolvedAccessor>builder();
            for (final var entry : childrenMap.entrySet()) {
                final var accessor = entry.getKey();
                resultBuilder.put(accessor.getOrdinal(), accessor);
            }
            return resultBuilder.build();
        }
    }

    /**
     * Trie data structure of {@link Type.Record.Field}s to conversion functions used to coerce an object of a certain type into
     * an object of another type.
     */
    public static class CoercionTrieNode extends TrieNode<ResolvedAccessor, Function<Object, Object>, CoercionTrieNode> {
        public CoercionTrieNode(@Nullable final Function<Object, Object> value, @Nullable final Map<ResolvedAccessor, CoercionTrieNode> childrenMap) {
            super(value, childrenMap);
        }

        @Nonnull
        @Override
        public CoercionTrieNode getThis() {
            return this;
        }
    }
}
