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
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.collect.Lists;
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
    protected static <M extends Message> Map<Tuple, List<DocumentEntry>> getRecordFields(@Nonnull KeyExpression root,
                                                                                         @Nullable FDBRecord<M> record) {
        Map<Tuple, List<DocumentEntry>> result = new HashMap<>();
        if (record != null) {
            // Grouping expression
            if (root instanceof GroupingKeyExpression) {
                GroupingKeyExpression group = (GroupingKeyExpression)root;
                getGroupedFields(Collections.singletonList(group.getWholeKey()), 0, 0,
                        group.getGroupingCount(), TupleHelpers.EMPTY, record, record.getRecord(), result);
            } else {
                result.put(TupleHelpers.EMPTY, getFields(root, record, record.getRecord()));
            }
        }
        return result;
    }

    // Grouping keys are evaluated more or less normally, turning into multiple groups.
    // Each group corresponds to a single document in a separate index / directory.
    // Within that document, the grouped fields are merged.
    protected static <M extends Message> void getGroupedFields(@Nonnull List<KeyExpression> keys, int keyIndex, int keyPosition,
                                                               int groupingCount, @Nonnull Tuple groupPrefix,
                                                               @Nonnull FDBRecord<M> record, @Nonnull Message message,
                                                               @Nonnull Map<Tuple, List<DocumentEntry>> result) {
        if (keyIndex >= keys.size()) {
            return;
        }
        KeyExpression key = keys.get(keyIndex);
        int keySize = key.getColumnSize();
        if (keyPosition + keySize <= groupingCount) {
            // Entirely in the grouping portion: extend group prefix with normal evaluation.
            List<Key.Evaluated> groups = key.evaluateMessage(record, message);
            for (Key.Evaluated group : groups) {
                Tuple wholeGroup = groupPrefix.addAll(group.toTupleAppropriateList());
                if (groupingCount == wholeGroup.size()) {
                    result.putIfAbsent(wholeGroup, new ArrayList<>());
                }
                getGroupedFields(keys, keyIndex + 1, keyPosition + key.getColumnSize(),
                        groupingCount, wholeGroup,
                        record, message, result);
            }
            return;
        }
        if (groupingCount <= keyPosition) {
            // Entirely in the grouped portion: add fields to groups.
            List<DocumentEntry> fields = getFields(key, record, message);
            for (Map.Entry<Tuple, List<DocumentEntry>> entry: result.entrySet()) {
                if (TupleHelpers.isPrefix(groupPrefix, entry.getKey())) {
                    entry.getValue().addAll(fields);
                }
            }
        // Grouping ends in the middle of this key: break it apart.
        } else if (key instanceof NestingKeyExpression) {
            NestingKeyExpression nesting = (NestingKeyExpression)key;
            for (Key.Evaluated value : nesting.getParent().evaluateMessage(record, message)) {
                final Message submessage = (Message) value.toList().get(0);
                getGroupedFields(Collections.singletonList(nesting.getChild()), 0, keyPosition,
                        groupingCount, groupPrefix, record, submessage, result);
            }
        } else if (key instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression)key;
            getGroupedFields(then.getChildren(), 0, keyPosition,
                    groupingCount, groupPrefix, record, message, result);
        } else {
            throw new RecordCoreException("Cannot split key for document grouping: " + key);
        }
        // Continue with remaining keys.
        getGroupedFields(keys, keyIndex + 1, keyPosition + key.getColumnSize(),
                groupingCount, groupPrefix, record, message, result);
    }

    @Nonnull
    public static <M extends Message> List<DocumentEntry> getFields(@Nonnull KeyExpression expression,
                                                                    @Nonnull FDBRecord<M> record, @Nonnull Message message) {
        if (expression instanceof ThenKeyExpression) {
            List<DocumentEntry> evaluatedList = Lists.newArrayList();
            for (KeyExpression child : ((ThenKeyExpression)expression).getChildren()) {
                evaluatedList.addAll(getFields(child, record, message));
            }
            if (!(expression instanceof LuceneThenKeyExpression) || ((LuceneThenKeyExpression)expression).getPrimaryKey() == null) {
                return evaluatedList;
            }
            DocumentEntry primaryKey = evaluatedList.get(((LuceneThenKeyExpression)expression).getPrimaryKeyPosition());
            evaluatedList.remove(primaryKey);
            List<DocumentEntry> result = Lists.newArrayList();
            String prefix = primaryKey.value.toString().concat("_");
            for (DocumentEntry entry : evaluatedList) {
                result.add(new DocumentEntry(prefix.concat(entry.fieldName), entry.value, entry.expression));
            }
            return result;
        }
        if (expression instanceof NestingKeyExpression) {
            final List<Key.Evaluated> parentKeys = ((NestingKeyExpression)expression).getParent().evaluateMessage(record, message);
            List<DocumentEntry> result = new ArrayList<>();
            for (Key.Evaluated value : parentKeys) {
                //TODO we may have to make this act like the NestingKeyExpression logic which takes only the first in the message list
                for (DocumentEntry entry : getFields(((NestingKeyExpression)expression).getChild(), record, (Message)value.toList().get(0))) {
                    if (((NestingKeyExpression)expression).getParent().getFanType().equals(KeyExpression.FanType.None)) {
                        result.add(new DocumentEntry(((NestingKeyExpression)expression).getParent().getFieldName().concat("_").concat(entry.fieldName), entry.value, entry.expression));
                    } else {
                        result.add(entry);
                    }
                }
            }
            return result;
        }
        //This should not happen
        if (expression instanceof GroupingKeyExpression) {
            return getFields(((GroupingKeyExpression)expression).getGroupedSubKey(), record, message);
        }
        if (expression instanceof LuceneFieldKeyExpression) {
            List<DocumentEntry> evaluatedKeys = Lists.newArrayList();
            for (Key.Evaluated key : expression.evaluateMessage(record, message)) {
                Object value = key.values().get(0);
                if (key.containsNonUniqueNull()) {
                    value = null;
                }
                evaluatedKeys.add(new DocumentEntry(((LuceneFieldKeyExpression)expression).getFieldName(), value, (LuceneFieldKeyExpression) expression));
            }
            return evaluatedKeys;
        }
        return Lists.newArrayList();
    }

    protected static class DocumentEntry {
        final String fieldName;
        final Object value;
        final LuceneFieldKeyExpression expression;

        DocumentEntry(String name, Object value, LuceneFieldKeyExpression expression) {
            this.fieldName = name;
            this.value = value;
            this.expression = expression;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final DocumentEntry entry = (DocumentEntry)o;
            return fieldName.equals(entry.fieldName) && Objects.equals(value, entry.value) && expression.equals(entry.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, value, expression);
        }

        @Override
        public String toString() {
            return fieldName + "=" + value;
        }
    }
}
