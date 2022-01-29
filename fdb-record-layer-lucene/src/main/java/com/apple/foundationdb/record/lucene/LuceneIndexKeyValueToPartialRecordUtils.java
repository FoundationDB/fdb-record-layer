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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;

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
                (source, fieldName, value, type, stored, fieldNamePrefix, suffixOverride, groupingKeyIndex) -> {
                    if (groupingKeyIndex > - 1) {
                        if (groupingKeyIndex > groupingKey.size() - 1) {
                            throw new RecordCoreException("Invalid grouping value tuple given a grouping key")
                                    .addLogInfo(LogMessageKeys.VALUE, groupingKey.toString());
                        }
                        source.buildMessage(groupingKey.get(groupingKeyIndex), (String) value);
                    } else if (type.equals(LuceneIndexExpressions.DocumentFieldType.TEXT) && fieldNameMatch(fieldName, luceneField, fieldNamePrefix, suffixOverride)) {
                        source.buildMessage(suggestion, (String) value);
                    }
                },
                null, 0, root instanceof GroupingKeyExpression ? ((GroupingKeyExpression) root).getGroupingCount() : 0);
    }

    private static boolean fieldNameMatch(@Nonnull String traversedFieldName, @Nonnull String givenFieldName,
                                          @Nullable String fieldNamePrefix, boolean suffixOverride) {
        // If field is not overridden, the names have to match exactly
        if (!suffixOverride) {
            return traversedFieldName.equals(givenFieldName);
        }
        // If field is overridden by a LUCENE_FIELD_NAME function, and there is a field name prefix, the given name should have same prefix to match
        if (fieldNamePrefix != null) {
            return givenFieldName.startsWith(fieldNamePrefix);
        }
        // Field is overridden by a LUCENE_FIELD_NAME function, and there is no field name prefix, so we assume the field match if the schema is reasonable
        return true;
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

        public Message buildMessage(@Nonnull Object value, String field) {
            return buildMessage(value, descriptor.findFieldByName(field));
        }

        private Message buildMessage(@Nonnull Object value, Descriptors.FieldDescriptor subFieldDescriptor) {
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

            if (parent == null) {
                return builder.build();
            } else {
                return parent.buildMessage(builder.build(), this.fieldDescriptor);
            }
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
