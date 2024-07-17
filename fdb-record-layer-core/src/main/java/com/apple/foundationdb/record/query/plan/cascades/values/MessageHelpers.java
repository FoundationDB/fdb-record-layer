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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.planprotos.PCoercionBiFunction;
import com.apple.foundationdb.record.planprotos.PCoercionTrieNode;
import com.apple.foundationdb.record.planprotos.PTransformationTrieNode;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.TrieNode;
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
import java.util.function.BiFunction;

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

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static boolean compareMessageEquals(@Nonnull Object o1, @Nonnull Object o2) {
        if (!(o1 instanceof Message) || !(o2 instanceof Message)) {
            return false;
        }
        MessageOrBuilder m1 = (MessageOrBuilder) o1;
        MessageOrBuilder m2 = (MessageOrBuilder) o2;

        if (m1.getDescriptorForType() == m2.getDescriptorForType()) {
            return m1.equals(m2);
        }

        if (m1.getDescriptorForType().getFields().size() != m2.getDescriptorForType().getFields().size()) {
            return false;
        }
        // compare FieldDescriptors one by one
        for (int i = 1; i <= m1.getDescriptorForType().getFields().size(); i++) {
            Descriptors.FieldDescriptor f1 = findFieldDescriptorOnMessage(m1, i);
            Descriptors.FieldDescriptor f2 = findFieldDescriptorOnMessage(m2, i);
            // do not support repeated or nested fields in the message
            Verify.verify(!f1.isRepeated() && !f1.isMapField() && f1.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE);
            Verify.verify(!f2.isRepeated() && !f2.isMapField() && f2.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE);
            if (f1.getJavaType() == f2.getJavaType() && m1.getField(f1).equals(m2.getField(f2))) {
                if (!m1.getField(f1).equals(m2.getField(f2))) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private static Message getFieldMessageOnMessage(@Nonnull MessageOrBuilder message, @Nonnull String fieldName) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessage(message, fieldName);
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

    /**
     * This method provides a structural deep copy of the {@link Message} passed in. This method was heavily inspired
     * by {@link Message.Builder#mergeFrom(Message)}, however, this method allow to also pass in a descriptor of describing
     * a compatible/equal message structure. In general, this method should always work (and work better), when
     * {@code DynamicMessage.parseFrom(targetDescriptor, other.toByteArray()} is well-defined.
     * Note that if {@code message.getDescriptorForType()} and {@code targetDescriptor} are incompatible in any way,
     * the behaviour/result of this method is undefined.
     *
     * @param targetDescriptor a descriptor that describes a structure that is wire-compatible with the {@code message}
     *        passed in
     * @param message a message
     * @return a new message of {@code targetDescriptor} which is a copy of the message passed in
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static Message deepCopyMessageIfNeeded(@Nonnull final Descriptors.Descriptor targetDescriptor, @Nonnull final Message message) {
        if (targetDescriptor == message.getDescriptorForType()) {
            return message;
        }

        final var builder = DynamicMessage.newBuilder(targetDescriptor);
        for (final var entry : message.getAllFields().entrySet()) {
            final Descriptors.FieldDescriptor field = entry.getKey();

            // find the field on the target side
            final var targetField = targetDescriptor.findFieldByNumber(field.getNumber());

            if (field.isRepeated()) {
                for (final var element : (List<?>)entry.getValue()) {
                    if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                        builder.addRepeatedField(targetField, deepCopyMessageIfNeeded(targetField.getMessageType(), (Message)element));
                    } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
                        final var enumValue = ((Descriptors.EnumValueDescriptor) element).getName();
                        builder.addRepeatedField(targetField, targetField.getEnumType().findValueByName(enumValue));
                    } else {
                        builder.addRepeatedField(targetField, element);
                    }
                }
            } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                final var existingValue = (Message)builder.getField(targetField);
                if (existingValue == existingValue.getDefaultInstanceForType()) {
                    builder.setField(field, entry.getValue());
                } else {
                    final var mergedObject =
                            DynamicMessage.newBuilder(targetField.getMessageType())
                                    .mergeFrom(deepCopyMessageIfNeeded(targetField.getMessageType(), existingValue))
                                    .mergeFrom(deepCopyMessageIfNeeded(targetField.getMessageType(), (Message)entry.getValue()))
                                    .build();
                    builder.setField(targetField, mergedObject);
                }
            } else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM) {
                final var enumValue = ((Descriptors.EnumValueDescriptor) entry.getValue()).getName();
                builder.setField(targetField, targetField.getEnumType().findValueByName(enumValue));
            } else {
                builder.setField(targetField, entry.getValue());
            }
        }
        builder.mergeUnknownFields(message.getUnknownFields());

        return builder.build();
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <M extends Message> Object transformMessage(@Nonnull final FDBRecordStoreBase<M> store,
                                                              @Nonnull final EvaluationContext context,
                                                              @Nullable final TransformationTrieNode transformationsTrie,
                                                              @Nullable final CoercionTrieNode coercionsTrie,
                                                              @Nonnull final Type targetType,
                                                              @Nonnull Descriptors.Descriptor targetDescriptor,
                                                              @Nonnull final Type currentType,
                                                              @Nonnull Descriptors.Descriptor currentDescriptor,
                                                              @Nullable final Object current) {
        final var value = transformationsTrie == null ? null : transformationsTrie.getValue();
        Verify.verify(value == null);

        final var targetDescriptorFields = targetDescriptor.getFields();
        final var targetRecordType = (Type.Record)targetType;
        final var currentRecordType = (Type.Record)currentType;

        final var transformationsChildrenMap = transformationsTrie == null ? null : transformationsTrie.getChildrenMap();
        final var coercionsChildrenMap = coercionsTrie == null ? null : coercionsTrie.getChildrenMap();
        final var currentMessage = (M)current;

        final var resultMessageBuilder = DynamicMessage.newBuilder(targetDescriptor);
        for (final var messageFieldDescriptor : currentDescriptor.getFields()) {
            final var index = messageFieldDescriptor.getIndex();
            final var transformationTrieForField = transformationsChildrenMap == null ? null : transformationsChildrenMap.get(index);
            final var targetFieldDescriptor = targetDescriptorFields.get(index);
            final var targetDescriptorForField = targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
                                                 ? targetFieldDescriptor.getMessageType()
                                                 : targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM
                                                   ? targetFieldDescriptor.getEnumType()
                                                   : null;
            final var promotionTrieForField = coercionsChildrenMap == null ? null : coercionsChildrenMap.get(index);
            final var targetFieldType = targetRecordType.getField(index).getFieldType();
            if (transformationTrieForField != null) {
                final var currentFieldType = currentRecordType.getField(index).getFieldType();
                Object fieldResult;
                if (transformationTrieForField.getValue() == null) {
                    Verify.verify(targetDescriptorForField instanceof Descriptors.Descriptor);

                    //
                    // Note that this recursive call has to happen even if the field value is null,
                    // as the transformations have to be done exhaustively, which then can create non-null values
                    // that have to be integrated into the message.
                    //
                    fieldResult =
                            transformMessage(store,
                                    context,
                                    transformationTrieForField,
                                    promotionTrieForField,
                                    targetFieldType,
                                    (Descriptors.Descriptor)targetDescriptorForField,
                                    currentFieldType,
                                    Verify.verifyNotNull(messageFieldDescriptor.getMessageType()),
                                    currentMessage == null ? null : currentMessage.getField(messageFieldDescriptor));
                } else {
                    final var transformationValue = transformationTrieForField.getValue();
                    fieldResult = coerceObject(promotionTrieForField,
                            targetFieldType,
                            targetDescriptorForField,
                            currentFieldType,
                            transformationValue.eval(store, context));
                }
                if (fieldResult != null) {
                    resultMessageBuilder.setField(targetFieldDescriptor, fieldResult);
                }
            } else {
                if (currentMessage != null) {
                    final var currentFieldType = Verify.verifyNotNull(currentRecordType.getField(messageFieldDescriptor.getIndex())).getFieldType();
                    final var fieldResult = NullableArrayTypeUtils.unwrapIfArray(getFieldOnMessage(currentMessage, messageFieldDescriptor), currentFieldType);
                    final var coercedObject =
                            coerceObject(promotionTrieForField, targetFieldType, targetDescriptorForField, currentFieldType, fieldResult);
                    if (coercedObject != null) {
                        resultMessageBuilder.setField(targetFieldDescriptor, coercedObject);
                    }
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
                                      @Nullable final Descriptors.GenericDescriptor targetDescriptor,
                                      @Nonnull final Type currentType,
                                      @Nullable final Object current) {
        SemanticException.check(current != null || targetType.isNullable(), SemanticException.ErrorCode.NULL_ASSIGNMENT);

        // In any case, a NULL returns a NULL
        if (current == null) {
            return null;
        }

        if (coercionsTrie == null) {
            if (targetDescriptor != null && current instanceof Message) {
                Verify.verify(targetDescriptor instanceof Descriptors.Descriptor);
                return deepCopyMessageIfNeeded((Descriptors.Descriptor)targetDescriptor, (Message)current);
            }
            return current;
        }

        //
        // This is the leaf case: If the target is primitive return the application of the coercion function to current
        // if the function exists; otherwise just return the object;
        //
        if (targetType.isPrimitive()) {
            Verify.verify(currentType.isPrimitive());
            final var coercionFunction = Verify.verifyNotNull(coercionsTrie.getValue());
            return Verify.verifyNotNull(coercionFunction.apply(null, current));
        }

        //
        // This is another leaf case: The target can be an Enum,
        if (targetType.isEnum()) {
            Verify.verify(targetDescriptor instanceof Descriptors.EnumDescriptor);
            final var coercionFunction = Verify.verifyNotNull(coercionsTrie.getValue());
            return Verify.verifyNotNull(coercionFunction.apply(targetDescriptor, current));
        }

        //
        // This juggles with a change in nullability for arrays. If we were nullable before, but now we are not or
        // vice versa, we need to change the wrapping in protobuf.
        //
        if (targetType.isArray()) {
            Verify.verify(currentType.isArray());
            final var coercionFunction = Verify.verifyNotNull(coercionsTrie.getValue());
            return Verify.verifyNotNull(coercionFunction.apply(targetDescriptor, current));
        }

        if (targetType.isRecord()) {
            Verify.verify(targetDescriptor instanceof Descriptors.Descriptor);
            return coerceMessage(coercionsTrie, targetType, Verify.verifyNotNull((Descriptors.Descriptor)targetDescriptor), currentType, (Message)current);
        }

        throw new IllegalStateException("unsupported java type for record field");
    }

    /**
     * Method to coerce an array.
     * This juggles with a change in nullability for arrays. If we were nullable before, but now we are not or
     * vice versa, we need to change the wrapping in protobuf.
     *
     * @param targetArrayType target array type
     * @param currentArrayType current array type
     * @param targetDescriptor target protobuf descriptor
     * @param elementsTrie a trie describing the coercions of the elements data structures
     * @param current the current object
     * @return a coerced array adjusted for nullability-differences of current versus target
     */
    @Nonnull
    public static Object coerceArray(@Nonnull final Type.Array targetArrayType,
                                     @Nonnull final Type.Array currentArrayType,
                                     @Nullable Descriptors.GenericDescriptor targetDescriptor,
                                     @Nullable final CoercionTrieNode elementsTrie,
                                     @Nonnull final Object current) {
        final var targetElementType = Verify.verifyNotNull(targetArrayType.getElementType());
        final var currentElementType = Verify.verifyNotNull(currentArrayType.getElementType());

        final Descriptors.FieldDescriptor targetElementFieldDescriptor;
        if (targetArrayType.isNullable()) {
            Verify.verify(targetDescriptor instanceof Descriptors.Descriptor);
            targetElementFieldDescriptor = Verify.verifyNotNull((Descriptors.Descriptor)targetDescriptor).findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName());
        } else {
            targetElementFieldDescriptor = null;
        }
        Descriptors.Descriptor targetDescriptorForElementField = null;
        if (targetElementFieldDescriptor != null) {
            targetDescriptorForElementField = targetElementFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ?
                                       targetElementFieldDescriptor.getMessageType() : null;
        }

        final var currentObjects = (List<?>)current;
        final var coercedObjectsBuilder = ImmutableList.builder();
        for (final var currentObject : currentObjects) {
            // NULL as elements of a collection are currently not supported
            SemanticException.check(currentObject != null, SemanticException.ErrorCode.UNSUPPORTED);

            final var coercedObject =
                    Verify.verifyNotNull(coerceObject(elementsTrie,
                            targetElementType,
                            targetElementFieldDescriptor == null ? targetDescriptor : targetDescriptorForElementField,
                            currentElementType,
                            currentObject));
            coercedObjectsBuilder.add(coercedObject);
        }
        final var coercedArray = coercedObjectsBuilder.build();

        if (targetArrayType.isNullable()) {
            // the target descriptor is the wrapping holder
            final var wrapperBuilder = DynamicMessage.newBuilder(Verify.verifyNotNull((Descriptors.Descriptor)targetDescriptor));
            wrapperBuilder.setField(Verify.verifyNotNull(targetElementFieldDescriptor), coercedArray);
            return wrapperBuilder.build();
        }
        return coercedArray;
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
            if (hasAnySuchField(currentMessage, messageFieldDescriptor)) {
                final var index = messageFieldDescriptor.getIndex();
                final var targetFieldDescriptor = Verify.verifyNotNull(targetFieldsFromDescriptor.get(index));
                final var targetFieldType = Verify.verifyNotNull(targetRecordType.getField(index)).getFieldType();
                final var currentField = currentRecordType.getField(index);
                final var currentFieldType = Verify.verifyNotNull(currentField).getFieldType();

                // coerced object can only be NULL if passed-in object is NULL which cannot happen here
                final var coercedObject =
                        Verify.verifyNotNull(
                                coerceObject(
                                        promotionsChildrenMap == null ? null : promotionsChildrenMap.get(index),
                                        targetFieldType,
                                        targetFieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ? targetFieldDescriptor.getMessageType() : null,
                                        currentFieldType,
                                        currentMessage.getField(messageFieldDescriptor)));
                resultMessageBuilder.setField(targetFieldDescriptor, coercedObject);
            }
        }
        return resultMessageBuilder.build();
    }

    private static boolean hasAnySuchField(@Nonnull final Message message, Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.isRepeated()) {
            return message.getRepeatedFieldCount(fieldDescriptor) > 0;
        } else {
            return message.hasField(fieldDescriptor);
        }
    }

    /**
     * Trie data structure of {@link Type.Record.Field}s to {@link Value}s.
     */
    public static class TransformationTrieNode extends TrieNode.AbstractTrieNode<Integer, Value, TransformationTrieNode> implements PlanHashable, PlanSerializable {

        public TransformationTrieNode(@Nullable final Value value, @Nullable final Map<Integer, TransformationTrieNode> childrenMap) {
            super(value, childrenMap);
        }

        @Nonnull
        @Override
        public TransformationTrieNode getThis() {
            return this;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            if (getChildrenMap() == null) {
                return PlanHashable.objectPlanHash(mode, getValue());
            }
            return PlanHashable.objectPlanHash(mode, getChildrenMap());
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean semanticEquals(final Object other, @Nonnull final AliasMap equivalencesMap) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TransformationTrieNode)) {
                return false;
            }
            final TransformationTrieNode otherTransformationTrieNode = (TransformationTrieNode)other;

            return equalsNullable(getValue(), otherTransformationTrieNode.getValue(), (t, o) -> t.semanticEquals(o, equivalencesMap)) &&
                   equalsNullable(getChildrenMap(), otherTransformationTrieNode.getChildrenMap(), (t, o) -> semanticEqualsForChildrenMap(t, o, equivalencesMap));
        }

        private static boolean semanticEqualsForChildrenMap(@Nonnull final Map<Integer, TransformationTrieNode> self,
                                                            @Nonnull final Map<Integer, TransformationTrieNode> other,
                                                            @Nonnull final AliasMap equivalencesMap) {
            if (self.size() != other.size()) {
                return false;
            }

            for (final var entry : self.entrySet()) {
                final var ordinal = entry.getKey();
                final var selfNestedTrie = entry.getValue();
                final var otherNestedTrie = other.get(ordinal);
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

        @Nonnull
        @Override
        public PTransformationTrieNode toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PTransformationTrieNode.Builder builder = PTransformationTrieNode.newBuilder();

            if (getValue() != null) {
                builder.setValue(getValue().toValueProto(serializationContext));
            }
            builder.setChildrenMapIsNull(getChildrenMap() == null);
            if (getChildrenMap() != null) {
                for (final Map.Entry<Integer, TransformationTrieNode> entry : getChildrenMap().entrySet()) {
                    builder.addChildPair(PTransformationTrieNode.IntChildPair.newBuilder()
                            .setIndex(entry.getKey())
                            .setChildTransformationTrieNode(entry.getValue().toProto(serializationContext)));
                }
            }
            return builder.build();
        }

        @Nonnull
        public static TransformationTrieNode fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PTransformationTrieNode transformationTrieNodeProto) {
            final Value value;
            if (transformationTrieNodeProto.hasValue()) {
                value =
                        Value.fromValueProto(serializationContext, transformationTrieNodeProto.getValue());
            } else {
                value = null;
            }

            Verify.verify(transformationTrieNodeProto.hasChildrenMapIsNull());

            final Map<Integer, TransformationTrieNode> childrenMap;
            if (!transformationTrieNodeProto.getChildrenMapIsNull()) {
                final ImmutableMap.Builder<Integer, TransformationTrieNode> childrenMapBuilder = ImmutableMap.builder();

                for (int i = 0; i < transformationTrieNodeProto.getChildPairCount(); i ++) {
                    final PTransformationTrieNode.IntChildPair childPair = transformationTrieNodeProto.getChildPair(i);
                    Verify.verify(childPair.hasIndex());
                    Verify.verify(childPair.hasChildTransformationTrieNode());
                    childrenMapBuilder.put(childPair.getIndex(),
                            TransformationTrieNode.fromProto(serializationContext, childPair.getChildTransformationTrieNode()));
                }
                childrenMap = childrenMapBuilder.build();
            } else {
                childrenMap = null;
            }
            return new TransformationTrieNode(value, childrenMap);
        }
    }

    /**
     * Trie data structure of {@link Type.Record.Field}s to conversion functions used to coerce an object of a certain type into
     * an object of another type.
     */
    public static class CoercionTrieNode extends TrieNode.AbstractTrieNode<Integer, CoercionBiFunction, CoercionTrieNode> implements PlanHashable, PlanSerializable {
        public CoercionTrieNode(@Nullable final CoercionBiFunction value, @Nullable final Map<Integer, CoercionTrieNode> childrenMap) {
            super(value, childrenMap);
        }

        @Nonnull
        @Override
        public CoercionTrieNode getThis() {
            return this;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            if (getChildrenMap() == null) {
                return PlanHashable.objectPlanHash(mode, getValue());
            }
            return PlanHashable.objectPlanHash(mode, getChildrenMap());
        }

        @Nonnull
        @Override
        public PCoercionTrieNode toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PCoercionTrieNode.Builder builder = PCoercionTrieNode.newBuilder();

            if (getValue() != null) {
                builder.setValue(getValue().toCoercionBiFunctionProto(serializationContext));
            }
            builder.setChildrenMapIsNull(getChildrenMap() == null);
            if (getChildrenMap() != null) {
                for (final Map.Entry<Integer, CoercionTrieNode> entry : getChildrenMap().entrySet()) {
                    builder.addChildPair(PCoercionTrieNode.IntChildPair.newBuilder()
                            .setIndex(entry.getKey())
                            .setChildCoercionTrieNode(entry.getValue().toProto(serializationContext)));
                }
            }
            return builder.build();
        }

        @Nonnull
        public static CoercionTrieNode fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PCoercionTrieNode coercionTrieNodeProto) {
            final CoercionBiFunction value;
            if (coercionTrieNodeProto.hasValue()) {
                value =
                        CoercionBiFunction.fromCoercionBiFunctionProto(serializationContext,
                                coercionTrieNodeProto.getValue());
            } else {
                value = null;
            }

            Verify.verify(coercionTrieNodeProto.hasChildrenMapIsNull());

            final Map<Integer, CoercionTrieNode> childrenMap;
            if (!coercionTrieNodeProto.getChildrenMapIsNull()) {
                final ImmutableMap.Builder<Integer, CoercionTrieNode> childrenMapBuilder = ImmutableMap.builder();

                for (int i = 0; i < coercionTrieNodeProto.getChildPairCount(); i ++) {
                    final PCoercionTrieNode.IntChildPair childPair = coercionTrieNodeProto.getChildPair(i);
                    Verify.verify(childPair.hasIndex());
                    Verify.verify(childPair.hasChildCoercionTrieNode());
                    childrenMapBuilder.put(childPair.getIndex(),
                            CoercionTrieNode.fromProto(serializationContext, childPair.getChildCoercionTrieNode()));
                }
                childrenMap = childrenMapBuilder.build();
            } else {
                childrenMap = null;
            }
            return new CoercionTrieNode(value, childrenMap);
        }
    }

    /**
     * Coercion (bi)-function which also is plan hashable.
     */
    public interface CoercionBiFunction extends BiFunction<Descriptors.GenericDescriptor, Object, Object>, PlanHashable, PlanSerializable {
        @Nonnull
        @SuppressWarnings("unused")
        PCoercionBiFunction toCoercionBiFunctionProto(@Nonnull PlanSerializationContext serializationContext);

        @Nonnull
        static CoercionBiFunction fromCoercionBiFunctionProto(@Nonnull final PlanSerializationContext serializationContext,
                                                              @Nonnull final PCoercionBiFunction coercionBiFunctionProto) {
            return (CoercionBiFunction)PlanSerialization.dispatchFromProtoContainer(serializationContext, coercionBiFunctionProto);
        }
    }
}
