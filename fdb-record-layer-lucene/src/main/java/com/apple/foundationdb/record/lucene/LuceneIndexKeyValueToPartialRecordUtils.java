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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
                (source, fieldName, value, type, stored, overriddenKeyRanges, groupingKeyIndex) -> {
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
        public Iterable<Object> getValues(@Nonnull FieldKeyExpression fieldExpression) {
            return Collections.singletonList(fieldExpression.getFieldName());
        }

        public void buildMessage(@Nullable Object value, @Nonnull String field, @Nullable String customizedKey, @Nullable String mappedKeyField, boolean forLuceneField) {
            if (hasBeenBuilt()) {
                return;
            }
            buildMessage(value, descriptor.findFieldByName(field), customizedKey, mappedKeyField, forLuceneField);
        }

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
                parent.buildMessage(builder.build(), this.fieldDescriptor, mappedKeyFieldDescriptor == null ? customizedKey : null, mappedKeyFieldDescriptor == null ? mappedKeyField : null, forLuceneField);
            }
            if (forLuceneField) {
                this.hasBeenBuilt = true;
            }
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
    public static class LuceneAutoCompleteCopier implements IndexKeyValueToPartialRecord.Copier {

        @Override
        public void copy(@Nonnull Descriptors.Descriptor recordDescriptor, @Nonnull Message.Builder recordBuilder,
                         @Nonnull IndexEntry kv) {
            Tuple keyTuple = kv.getKey();
            if (keyTuple.size() < 2) {
                throw new RecordCoreException("Invalid key tuple for auto-complete suggestion's index entry")
                        .addLogInfo(LogMessageKeys.KEY_SIZE, keyTuple.size())
                        .addLogInfo(LogMessageKeys.RECORD_TYPE, recordDescriptor.getName());
            }
            String fieldName = (String) keyTuple.get(keyTuple.size() - 2);
            String value = (String) keyTuple.get(keyTuple.size() - 1);
            Tuple groupingKey = Tuple.fromList(keyTuple.getItems().subList(0, keyTuple.size() - 2));
            buildPartialRecord(kv.getIndex().getRootExpression(), recordDescriptor, recordBuilder, fieldName, value, groupingKey);
        }
    }
}
