/*
 * LuceneDocumentFromRecord.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class for converting {@link FDBRecord}s to Lucene documents.
 */
public class LuceneDocumentFromRecord {
    private LuceneDocumentFromRecord() {
    }

    @Nonnull
    protected static <M extends Message> Map<Tuple, List<DocumentField>> getRecordFields(@Nonnull KeyExpression root,
                                                                                         @Nullable FDBRecord<M> rec) {
        Map<Tuple, List<DocumentField>> result = new HashMap<>();
        if (rec != null) {
            // Grouping expression
            if (root instanceof GroupingKeyExpression) {
                GroupingKeyExpression group = (GroupingKeyExpression)root;
                getGroupedFields(Collections.singletonList(group.getWholeKey()), 0, 0,
                        group.getGroupingCount(), TupleHelpers.EMPTY, rec, rec.getRecord(), result, null);
            } else {
                result.put(TupleHelpers.EMPTY, getFields(root, rec, rec.getRecord(), null));
            }
        }
        return result;
    }

    // Grouping keys are evaluated more or less normally, turning into multiple groups.
    // Each group corresponds to a single document in a separate index / directory.
    // Within that document, the grouped fields are merged.
    protected static <M extends Message> void getGroupedFields(@Nonnull List<KeyExpression> keys, int keyIndex, int keyPosition,
                                                               int groupingCount, @Nonnull Tuple groupPrefix,
                                                               @Nonnull FDBRecord<M> rec, @Nonnull Message message,
                                                               @Nonnull Map<Tuple, List<DocumentField>> result,
                                                               @Nullable String fieldNamePrefix) {
        if (keyIndex >= keys.size()) {
            return;
        }
        KeyExpression key = keys.get(keyIndex);
        int keySize = key.getColumnSize();
        if (keyPosition + keySize <= groupingCount) {
            // Entirely in the grouping portion: extend group prefix with normal evaluation.
            List<Key.Evaluated> groups = key.evaluateMessage(rec, message);
            for (Key.Evaluated group : groups) {
                Tuple wholeGroup = groupPrefix.addAll(group.toTupleAppropriateList());
                if (groupingCount == wholeGroup.size()) {
                    result.putIfAbsent(wholeGroup, new ArrayList<>());
                }
                getGroupedFields(keys, keyIndex + 1, keyPosition + key.getColumnSize(),
                        groupingCount, wholeGroup,
                        rec, message, result, fieldNamePrefix);
            }
            return;
        }
        if (groupingCount <= keyPosition) {
            // Entirely in the grouped portion: add fields to groups.
            List<DocumentField> fields = getFields(key, rec, message, fieldNamePrefix);
            for (Map.Entry<Tuple, List<DocumentField>> entry: result.entrySet()) {
                if (TupleHelpers.isPrefix(groupPrefix, entry.getKey())) {
                    entry.getValue().addAll(fields);
                }
            }
        // Grouping ends in the middle of this key: break it apart.
        } else if (key instanceof NestingKeyExpression) {
            NestingKeyExpression nesting = (NestingKeyExpression)key;
            final String parentFieldName = nesting.getParent().getFieldName();
            for (Key.Evaluated value : nesting.getParent().evaluateMessage(rec, message)) {
                final Message submessage = (Message) value.toList().get(0);
                getGroupedFields(Collections.singletonList(nesting.getChild()), 0, keyPosition,
                        groupingCount, groupPrefix, rec, submessage, result,
                        fieldNamePrefix == null ? parentFieldName : fieldNamePrefix + "_" + parentFieldName);
            }
        } else if (key instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression)key;
            getGroupedFields(then.getChildren(), 0, keyPosition,
                    groupingCount, groupPrefix, rec, message, result, fieldNamePrefix);
        } else {
            throw new RecordCoreException("Cannot split key for document grouping: " + key);
        }
        // Continue with remaining keys.
        getGroupedFields(keys, keyIndex + 1, keyPosition + key.getColumnSize(),
                groupingCount, groupPrefix, rec, message, result, fieldNamePrefix);
    }

    @Nonnull
    public static <M extends Message> List<DocumentField> getFields(@Nonnull KeyExpression expression, @Nonnull FDBRecord<M> rec,
                                                                    @Nonnull Message message, @Nullable String fieldNamePrefix) {
        final DocumentFieldList<FDBRecordSource<M>> fields = new DocumentFieldList<>();
        LuceneIndexExpressions.getFields(expression, new FDBRecordSource<>(rec, message), fields, fieldNamePrefix);
        return fields.getFields();
    }

    /**
     * A RecordSource based on an FDBRecord.
     *
     * @param <M> the message type contained in the source.
     */
    public static class FDBRecordSource<M extends Message> implements LuceneIndexExpressions.RecordSource<FDBRecordSource<M>> {
        @Nonnull
        private final FDBRecord<M> rec;
        @Nonnull
        private final Message message;

        public FDBRecordSource(@Nonnull final FDBRecord<M> rec, @Nonnull final Message message) {
            this.rec = rec;
            this.message = message;
        }

        @Nonnull
        public Message getMessage() {
            return message;
        }

        @Override
        public Descriptors.Descriptor getDescriptor() {
            return message.getDescriptorForType();
        }

        @Override
        public Iterable<FDBRecordSource<M>> getChildren(final FieldKeyExpression parentExpression) {
            final List<FDBRecordSource<M>> children = new ArrayList<>();
            for (Key.Evaluated evaluated : parentExpression.evaluateMessage(rec, message)) {
                final Message submessage = (Message)evaluated.toList().get(0);
                if (submessage != null) {
                    children.add(new FDBRecordSource<>(rec, submessage));
                }
            }
            return children;
        }

        @Override
        public Iterable<Object> getValues(final KeyExpression keyExpression) {
            final List<Object> values = new ArrayList<>();
            for (Key.Evaluated evaluated : keyExpression.evaluateMessage(rec, message)) {
                Object value = evaluated.getObject(0);
                if (value != null) {
                    values.add(value);
                }
            }
            return values;
        }
    }

    protected static class DocumentFieldList<T extends LuceneIndexExpressions.RecordSource<T>> implements LuceneIndexExpressions.DocumentDestination<T> {
        private final List<DocumentField> fields = new ArrayList<>();

        public List<DocumentField> getFields() {
            return fields;
        }

        @Override
        public void addField(@Nonnull T source, @Nonnull String fieldName, @Nullable final Object value,
                                      @Nonnull LuceneIndexExpressions.DocumentFieldType type,
                                      final boolean fieldNameOverride, @Nullable List<String> namedFieldPath, @Nullable String namedFieldSuffix,
                                      boolean stored, boolean sorted,
                                      @Nonnull List<Integer> overriddenKeyRanges, int groupingKeyIndex, int keyIndex,
                                      @Nonnull Map<String, Object> fieldConfigs) {
            fields.add(new DocumentField(fieldName, value, type, stored, sorted, fieldConfigs));
        }
    }

    public static class DocumentField {
        @Nonnull
        private final String fieldName;
        @Nullable
        private final Object value;
        private final LuceneIndexExpressions.DocumentFieldType type;
        private final boolean stored;
        private final boolean sorted;
        @Nonnull
        private final Map<String, Object> fieldConfigs;

        public DocumentField(@Nonnull String fieldName, @Nullable Object value, LuceneIndexExpressions.DocumentFieldType type,
                             boolean stored, boolean sorted, @Nonnull Map<String, Object> fieldConfigs) {
            this.fieldName = fieldName;
            this.value = value;
            this.type = type;
            this.stored = stored;
            this.sorted = sorted;
            this.fieldConfigs = fieldConfigs;
        }

        @Nonnull
        public String getFieldName() {
            return fieldName;
        }

        @Nullable
        public Object getValue() {
            return value;
        }

        public LuceneIndexExpressions.DocumentFieldType getType() {
            return type;
        }

        public boolean isStored() {
            return stored;
        }

        public boolean isSorted() {
            return sorted;
        }

        @Nonnull
        public Map<String, Object> getFieldConfigs() {
            return Collections.unmodifiableMap(fieldConfigs);
        }

        @Nullable
        public Object getConfig(@Nonnull String key) {
            return fieldConfigs.get(key);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final DocumentField that = (DocumentField)o;
            if (stored != that.stored) {
                return false;
            }
            if (sorted != that.sorted) {
                return false;
            }
            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            if (!Objects.equals(value, that.value)) {
                return false;
            }
            if (type != that.type) {
                return false;
            }
            return Objects.equals(fieldConfigs, that.fieldConfigs);
        }

        @Override
        public int hashCode() {
            int result = fieldName.hashCode();
            result = 31 * result + (value != null ? value.hashCode() : 0);
            result = 31 * result + type.hashCode();
            result = 31 * result + (stored ? 1 : 0);
            result = 31 * result + (sorted ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return fieldName + "=" + value;
        }
    }
    
}
