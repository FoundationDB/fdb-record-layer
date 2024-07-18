/*
 * LuceneIndexKeyValueToPartialRecordUtils.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PCopier;
import com.apple.foundationdb.record.planprotos.PLuceneSpellCheckCopier;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.planprotos.LuceneRecordQueryPlanProto.luceneSpellCheckCopier;

/**
 * A utility class to build a partial record for an auto-complete suggestion value, with grouping keys if there exist.
 */
public class LuceneIndexKeyValueToPartialRecordUtils {
    private LuceneIndexKeyValueToPartialRecordUtils() {
    }

    public static void buildPartialRecord(@Nonnull KeyExpression root, @Nonnull Descriptors.Descriptor descriptor,
                                          @Nonnull Message.Builder builder, @Nonnull String luceneField,
                                          @Nonnull String suggestion) {
        buildPartialRecord(root, descriptor, builder, luceneField, suggestion, TupleHelpers.EMPTY);
    }

    public static void buildPartialRecord(@Nonnull KeyExpression root, @Nonnull Descriptors.Descriptor descriptor,
                                          @Nonnull Message.Builder builder, @Nonnull String luceneField,
                                          @Nonnull String suggestion, @Nonnull Tuple groupingKey) {
        final KeyExpression expression = root instanceof GroupingKeyExpression ? ((GroupingKeyExpression) root).getWholeKey() : root;
        LuceneIndexExpressions.getFieldsRecursively(expression, new PartialRecordBuildSource(null, descriptor, builder),
                (source, fieldName, value, type, fieldNameOverride, namedFieldPath, namedFieldSuffix, stored, sorted, overriddenKeyRanges, groupingKeyIndex, keyIndex, fieldConfigsIgnored) -> {
                    if (groupingKeyIndex > - 1) {
                        if (groupingKeyIndex > groupingKey.size() - 1) {
                            throw new RecordCoreException("Invalid grouping value tuple given a grouping key")
                                    .addLogInfo(LogMessageKeys.VALUE, groupingKey.toString());
                        }
                        source.buildMessage(groupingKey.get(groupingKeyIndex), (String) value, null, null, false);
                    } else if (type.equals(LuceneIndexExpressions.DocumentFieldType.TEXT)) {
                        buildIfFieldNameMatch(source, fieldName, luceneField, overriddenKeyRanges, suggestion, (String) value);
                    }
                },
                null, 0, root instanceof GroupingKeyExpression ? ((GroupingKeyExpression) root).getGroupingCount() : 0, new ArrayList<>());
    }

    public static void populatePrimaryKey(@Nonnull KeyExpression primaryKey, @Nonnull Descriptors.Descriptor descriptor, @Nonnull Message.Builder builder, @Nonnull Tuple tuple) {
        LuceneIndexExpressions.getFields(primaryKey, new PartialRecordBuildSource(null, descriptor, builder),
                (source, fieldName, value, type, fieldNameOverride, namedFieldPath, namedFieldSuffix, stored, sorted, overriddenKeyRanges, groupingKeyIndex, keyIndex, fieldConfigsIgnored) -> {
                    source.buildMessage(tuple.get(keyIndex), (String) value, null, null, false);
                }, null);
    }

    private static void buildIfFieldNameMatch(@Nonnull PartialRecordBuildSource source, @Nonnull String concatenatedFieldPath, @Nonnull String givenFieldName,
                                                 @Nonnull List<Integer> overriddenKeyRanges, @Nonnull String suggestion, @Nonnull String protoFieldName) {
        // If field is not overridden, the names have to match exactly
        if (overriddenKeyRanges.isEmpty()) {
            if (concatenatedFieldPath.equals(givenFieldName)) {
                source.buildMessage(suggestion, protoFieldName, null, null, true);
            }
            return;
        }
        // They can still be equal if some keys are mapped to fixed value or NULL
        if (concatenatedFieldPath.equals(givenFieldName)) {
            source.buildMessage(suggestion, protoFieldName, null, null, true);
            return;
        }

        Pair<List<String>, List<String>> pair = getOriginalAndMappedFieldElements(concatenatedFieldPath, overriddenKeyRanges);
        final List<String> fixedFieldNames = pair.getLeft();
        final List<String> dynamicFieldNames = pair.getRight();

        // If the given field name contain all the fixed names from the path, we conclude that it matches
        String fieldName = givenFieldName;
        for (String fixedField : fixedFieldNames) {
            int index = fieldName.indexOf(fixedField);
            if (index < 0) {
                return;
            }
            if (index + fixedField.length() < fieldName.length() && fieldName.charAt(index + fixedField.length()) != '_') {
                return;
            }
            fieldName = fieldName.substring(index + fixedField.length());
        }

        // If the given field name matches with the generated field path, then to find the customized key for the Lucene field from given Lucene field name
        String customizedKeyForLuceneField;
        String mappedKeyField = dynamicFieldNames.get(dynamicFieldNames.size() - 1);
        if (fixedFieldNames.isEmpty()) {
            customizedKeyForLuceneField = givenFieldName;
        } else if (fixedFieldNames.size() == 1) {
            final String lastOriginalField = fixedFieldNames.get(fixedFieldNames.size() - 1);
            if (givenFieldName.endsWith(lastOriginalField)) {
                if (dynamicFieldNames.size() == 1) {
                    customizedKeyForLuceneField = givenFieldName.substring(0, givenFieldName.lastIndexOf(lastOriginalField) - 1);
                } else {
                    int index = 0;
                    while (index <= givenFieldName.lastIndexOf(lastOriginalField) - 1) {
                        if (givenFieldName.charAt(index) == concatenatedFieldPath.charAt(index)) {
                            index++;
                        } else {
                            break;
                        }
                    }
                    customizedKeyForLuceneField = givenFieldName.substring(index, givenFieldName.lastIndexOf(lastOriginalField) - 1);
                }
            } else {
                customizedKeyForLuceneField = givenFieldName.substring(givenFieldName.lastIndexOf(lastOriginalField) + lastOriginalField.length() + 1);
            }
        } else {
            final String penultimateOriginalField = fixedFieldNames.get(fixedFieldNames.size() - 2);
            final String lastOriginalField = fixedFieldNames.get(fixedFieldNames.size() - 1);
            if (givenFieldName.endsWith(lastOriginalField)) {
                if (penultimateOriginalField.equals(lastOriginalField)) {
                    int endIndex = givenFieldName.lastIndexOf(lastOriginalField) - 1;
                    int startIndex = givenFieldName.substring(0, endIndex).lastIndexOf(penultimateOriginalField) + penultimateOriginalField.length() + 1;
                    customizedKeyForLuceneField = givenFieldName.substring(startIndex, endIndex);
                } else {
                    int startIndex = givenFieldName.lastIndexOf(penultimateOriginalField) + penultimateOriginalField.length() + 1;
                    int endIndex = givenFieldName.lastIndexOf(lastOriginalField) - 1;
                    customizedKeyForLuceneField = givenFieldName.substring(startIndex, endIndex);
                }
            } else {
                customizedKeyForLuceneField = givenFieldName.substring(givenFieldName.lastIndexOf(lastOriginalField) + lastOriginalField.length() + 1);
            }
        }
        
        source.buildMessage(suggestion, protoFieldName, customizedKeyForLuceneField, mappedKeyField, true);
    }


    /**
     * Get the list of fixed field names and that of the dynamic field names, given the entire concatenated field name.
     * @param entireFieldName the entire concatenated field name
     * @param overriddenKeyRanges the position ranges of the dynamic field names within the entire field name
     * @return a pair of the list of fixed names and that of the dynamic ones
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Pair<List<String>, List<String>> getOriginalAndMappedFieldElements(@Nonnull String entireFieldName,
                                                                                      @Nonnull List<Integer> overriddenKeyRanges) {
        int size = overriddenKeyRanges.size();
        final List<String> fixedFieldNames = new ArrayList<>();
        final List<String> dynamicFieldNames = new ArrayList<>();
        if (overriddenKeyRanges.get(0) > 0) {
            int startIndex = 0;
            int endIndex = overriddenKeyRanges.get(0) > entireFieldName.length() - 1 ? entireFieldName.length() : overriddenKeyRanges.get(0) - 1;
            if (endIndex >= startIndex) {
                fixedFieldNames.add(entireFieldName.substring(startIndex, endIndex));
            }
        }
        for (int i = 0; i < size - 1; i = i + 2) {
            boolean keyIsNull = overriddenKeyRanges.get(i) == overriddenKeyRanges.get(i + 1);
            if (keyIsNull) {
                dynamicFieldNames.add("");
            } else {
                dynamicFieldNames.add(entireFieldName.substring(overriddenKeyRanges.get(i), overriddenKeyRanges.get(i + 1)));
            }
            if (i < size - 3) {
                int startIndex = overriddenKeyRanges.get(i + 1) == 0 ? overriddenKeyRanges.get(i + 1) : overriddenKeyRanges.get(i + 1) + 1;
                int endIndex = overriddenKeyRanges.get(i + 2) == entireFieldName.length() ? overriddenKeyRanges.get(i + 2) : overriddenKeyRanges.get(i + 2) - 1;
                if (endIndex >= startIndex) {
                    fixedFieldNames.add(entireFieldName.substring(startIndex, endIndex));
                }
            }
        }
        if (overriddenKeyRanges.get(size - 1) < entireFieldName.length()) {
            int startIndex = overriddenKeyRanges.get(size - 1) == 0 ? overriddenKeyRanges.get(size - 1) : overriddenKeyRanges.get(size - 1) + 1;
            if (entireFieldName.length() >= startIndex) {
                fixedFieldNames.add(entireFieldName.substring(startIndex));
            }
        }

        return Pair.of(fixedFieldNames, dynamicFieldNames);
    }

    /**
     * Get the {@link IndexKeyValueToPartialRecord} instance for an {@link IndexEntry} representing a result of Lucene auto-complete suggestion.
     * The partial record contains the suggestion in the field where it is indexed from, and the grouping keys if there are any.
     * @param index the index being scanned
     * @param recordType the record type for indexed records
     * @param scanType the type of scan
     * @return a partial record generator
     */
    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @VisibleForTesting
    public static IndexKeyValueToPartialRecord getToPartialRecord(@Nonnull Index index,
                                                                  @Nonnull RecordType recordType,
                                                                  @Nonnull IndexScanType scanType) {
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);

        KeyExpression root = index.getRootExpression();
        final int groupingColumnSize;
        if (root instanceof GroupingKeyExpression) {
            KeyExpression groupingKey = ((GroupingKeyExpression) root).getGroupingSubKey();
            groupingColumnSize = groupingKey.getColumnSize();
            for (int i = 0; i < groupingKey.getColumnSize(); i++) {
                AvailableFields.addCoveringField(groupingKey, AvailableFields.FieldData.ofUnconditional(IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(i)), builder);
            }
        } else {
            groupingColumnSize = 0;
        }

        builder.addRequiredMessageFields();
        if (!builder.isValid(true)) {
            throw new RecordCoreException("Missing required field for result record")
                    .addLogInfo(LogMessageKeys.INDEX_NAME, index.getName())
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, recordType.getName())
                    .addLogInfo(LogMessageKeys.SCAN_TYPE, scanType);
        }

        builder.addRegularCopier(
                new LuceneSpellCheckCopier(groupingColumnSize));

        return builder.build();
    }

    /**
     * A {@link com.apple.foundationdb.record.lucene.LuceneIndexExpressions.RecordSource} implementation to build the partial record message.
     */
    static class PartialRecordBuildSource implements LuceneIndexExpressions.RecordSource<PartialRecordBuildSource> {
        @Nullable
        private final PartialRecordBuildSource parent;
        @Nonnull
        private final Descriptors.Descriptor descriptor;
        @Nullable
        private final Descriptors.FieldDescriptor fieldDescriptor;
        @Nonnull
        private final Message.Builder builder;
        private boolean hasBeenBuilt = false;

        PartialRecordBuildSource(@Nullable PartialRecordBuildSource parent, @Nonnull Descriptors.Descriptor descriptor, @Nonnull Message.Builder builder) {
            this.parent = parent;
            this.descriptor = descriptor;
            this.fieldDescriptor = null;
            this.builder = builder;
        }

        PartialRecordBuildSource(@Nullable PartialRecordBuildSource parent, @Nonnull Descriptors.FieldDescriptor fieldDescriptor, @Nonnull Message.Builder builder) {
            this.parent = parent;
            this.descriptor = fieldDescriptor.getMessageType();
            this.fieldDescriptor = fieldDescriptor;
            this.builder = builder;
        }

        @Override
        public Descriptors.Descriptor getDescriptor() {
            return descriptor;
        }

        @Override
        public Iterable<PartialRecordBuildSource> getChildren(@Nonnull FieldKeyExpression parentExpression) {
            final String parentField = parentExpression.getFieldName();
            final Descriptors.FieldDescriptor parentFieldDescriptor = descriptor.findFieldByName(parentField);
            return Collections.singletonList(new PartialRecordBuildSource(this, parentFieldDescriptor, builder.newBuilderForField(parentFieldDescriptor)));
        }

        @Override
        public Iterable<Object> getValues(@Nonnull KeyExpression keyExpression) {
            List<Object> result = new ArrayList<>();
            KeyExpression current = keyExpression;
            while (current != null) {
                if (current instanceof NestingKeyExpression) {
                    NestingKeyExpression expression = (NestingKeyExpression)current;
                    result.add(expression.getParent().getFieldName());
                    current = expression.getChild();
                } else if (current instanceof FieldKeyExpression) {
                    result.add(((FieldKeyExpression)current).getFieldName());
                    current = null;
                } else {
                    throw new RecordCoreArgumentException("Nested key type not supported for values")
                            .addLogInfo("keyType", current.getClass().getName());
                }
            }
            return result;
        }

        public void buildMessage(@Nullable Object value, @Nonnull String field, @Nullable String customizedKey, @Nullable String mappedKeyField, boolean forLuceneField) {
            if (hasBeenBuilt()) {
                return;
            }
            buildMessage(value, descriptor.findFieldByName(field), customizedKey, mappedKeyField, forLuceneField);
        }

        @SuppressWarnings("java:S3776")
        private void buildMessage(@Nullable Object value, Descriptors.FieldDescriptor subFieldDescriptor, @Nullable String customizedKey, @Nullable String mappedKeyField, boolean forLuceneField) {
            final Descriptors.FieldDescriptor mappedKeyFieldDescriptor = mappedKeyField == null ? null : descriptor.findFieldByName(mappedKeyField);
            if (mappedKeyFieldDescriptor != null) {
                if (customizedKey == null) {
                    return;
                }
                builder.setField(mappedKeyFieldDescriptor, customizedKey);
            }

            if (value == null) {
                return;
            }
            if (subFieldDescriptor.isRepeated()) {
                if (builder.getRepeatedFieldCount(subFieldDescriptor) > 0) {
                    Message.Builder subBuilder = builder.newBuilderForField(subFieldDescriptor);
                    subBuilder
                            .mergeFrom((Message) builder.getRepeatedField(subFieldDescriptor, 0))
                            .mergeFrom((Message) value);
                    builder.setRepeatedField(subFieldDescriptor, 0, subBuilder.build());
                } else {
                    builder.addRepeatedField(subFieldDescriptor, value);
                }
            } else {
                builder.setField(subFieldDescriptor, value);
            }

            if (parent != null) {
                addRequiredFieldsToBuilder(builder);
                parent.buildMessage(builder.build(), this.fieldDescriptor, mappedKeyFieldDescriptor == null ? customizedKey : null, mappedKeyFieldDescriptor == null ? mappedKeyField : null, forLuceneField);
            }
            if (forLuceneField) {
                this.hasBeenBuilt = true;
            }
        }

        private static MessageLite.Builder addRequiredFieldsToBuilder(@Nonnull final Message.Builder builder) {
            final var recordDescriptor = builder.getDescriptorForType();
            for (Descriptors.FieldDescriptor fieldDescriptor : recordDescriptor.getFields()) {
                if (fieldDescriptor.isRequired() &&
                        fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE &&
                        !builder.hasField(fieldDescriptor)) {
                    final var fieldBuilder = builder.newBuilderForField(fieldDescriptor);
                    builder.setField(fieldDescriptor, addRequiredFieldsToBuilder(fieldBuilder).build());
                }
            }
            return builder;
        }

        private boolean hasBeenBuilt() {
            if (this.hasBeenBuilt) {
                return true;
            }
            if (this.parent != null) {
                return this.parent.hasBeenBuilt;
            }
            return false;
        }

    }

    /**
     * The copier to populate the lucene auto complete suggestion as a value for the field where it is indexed from.
     * So the suggestion can be returned as a {@link com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord} for query.
     */
    public static class LuceneSpellCheckCopier implements IndexKeyValueToPartialRecord.Copier {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Spell-Check-Copier");
        private final int groupingColumnSize;

        public LuceneSpellCheckCopier(final int groupingColumnSize) {
            this.groupingColumnSize = groupingColumnSize;
        }

        @Override
        public boolean copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                            @Nonnull IndexEntry kv) {
            Tuple keyTuple = kv.getKey();
            if (keyTuple.size() < 2) {
                throw new RecordCoreException("Invalid key tuple for auto-complete suggestion's index entry")
                        .addLogInfo(LogMessageKeys.KEY_SIZE, keyTuple.size())
                        .addLogInfo(LogMessageKeys.RECORD_TYPE, recordDescriptor.getName());
            }
            Tuple groupingKey = Tuple.fromList(keyTuple.getItems().subList(0, groupingColumnSize));
            String fieldName = (String) keyTuple.get(groupingColumnSize);
            String value = (String) keyTuple.get(groupingColumnSize + 1);
            buildPartialRecord(kv.getIndex().getRootExpression(), recordDescriptor, recordBuilder, fieldName, value, groupingKey);
            return true;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LuceneSpellCheckCopier)) {
                return false;
            }
            final LuceneSpellCheckCopier that = (LuceneSpellCheckCopier)o;
            return groupingColumnSize == that.groupingColumnSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(BASE_HASH, groupingColumnSize);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, groupingColumnSize);
        }

        @Nonnull
        @Override
        public PLuceneSpellCheckCopier toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PLuceneSpellCheckCopier.newBuilder()
                    .setGroupingColumnSize(groupingColumnSize)
                    .build();
        }

        @Nonnull
        @Override
        public PCopier toCopierProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PCopier.newBuilder()
                    .setExtension(luceneSpellCheckCopier, toProto(serializationContext))
                    .build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static LuceneSpellCheckCopier fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PLuceneSpellCheckCopier luceneSpellCheckCopierProto) {
            return new LuceneSpellCheckCopier(PlanSerialization.getFieldOrThrow(luceneSpellCheckCopierProto,
                    PLuceneSpellCheckCopier::hasGroupingColumnSize,
                    PLuceneSpellCheckCopier::getGroupingColumnSize));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PLuceneSpellCheckCopier, LuceneSpellCheckCopier> {
            @Nonnull
            @Override
            public Class<PLuceneSpellCheckCopier> getProtoMessageClass() {
                return PLuceneSpellCheckCopier.class;
            }

            @Nonnull
            @Override
            public LuceneSpellCheckCopier fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PLuceneSpellCheckCopier luceneSpellCheckCopierProto) {
                return LuceneSpellCheckCopier.fromProto(serializationContext, luceneSpellCheckCopierProto);
            }
        }
    }
}
