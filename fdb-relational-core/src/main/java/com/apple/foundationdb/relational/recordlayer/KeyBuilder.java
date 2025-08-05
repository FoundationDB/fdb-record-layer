/*
 * KeyBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.base.Joiner;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.stream.IntStream;

@API(API.Status.EXPERIMENTAL)
public class KeyBuilder {
    private final RecordType typeForKey;
    private final KeyExpression key;
    private final String scannableNameForMessage;

    public KeyBuilder(RecordType typeForKey, KeyExpression key, String scannableName) {
        this.key = key;
        this.typeForKey = typeForKey;
        this.scannableNameForMessage = scannableName;
    }

    public int getKeySize() {
        return key.getColumnSize();
    }

    @Nonnull
    public Row buildKey(Map<String, Object> keyFields, boolean failOnIncompleteKey) throws RelationalException {
        Map<String, Object> keysNotPicked = new HashMap<>(keyFields);
        List<Object> flattenedFields = new ArrayList<>();

        for (Object key : flattenKeys()) {
            if (key instanceof RecordType) {
                flattenedFields.add(((RecordType) key).getRecordTypeKey());
            } else if (key instanceof String) {
                Object value = keyFields.get(key);
                if (value != null) {
                    keysNotPicked.remove(key);
                }
                flattenedFields.add(value);
            } else {
                Assert.fail("Should never happen");
            }
        }

        if (failOnIncompleteKey && flattenedFields.stream().anyMatch(Objects::isNull)) {
            int missing = IntStream.range(0, flattenedFields.size()).filter(i -> flattenedFields.get(i) == null).findFirst().getAsInt();
            throw new RelationalException("Cannot form incomplete key: missing key at position <" + missing + ">", ErrorCode.INVALID_PARAMETER);
        }

        if (!keysNotPicked.isEmpty()) {
            throw new RelationalException("Unknown keys for " + scannableNameForMessage + ", unknown keys: <" + Joiner.on(",").join(keysNotPicked.keySet()) + ">",
                    ErrorCode.INVALID_PARAMETER);
        }

        // Remove trailing nulls
        while (!flattenedFields.isEmpty() && flattenedFields.get(flattenedFields.size() - 1) == null) {
            flattenedFields.remove(flattenedFields.size() - 1);
        }

        if (flattenedFields.stream().anyMatch(Objects::isNull)) {
            int missing = IntStream.range(0, flattenedFields.size()).filter(i -> flattenedFields.get(i) == null).findFirst().getAsInt();
            throw new RelationalException("Cannot form key: missing key at position <" + missing + ">", ErrorCode.INVALID_PARAMETER);
        }

        return new FDBTuple(Tuple.fromList(flattenedFields));
    }

    @Nonnull
    public Row buildKey(Row scannedRow) throws RelationalException {
        int scannedIndex = 0;
        List<Object> flattenedFields = new ArrayList<>();
        for (Object key : flattenKeys()) {
            if (key instanceof RecordType) {
                flattenedFields.add(((RecordType) key).getRecordTypeKey());
            } else {
                flattenedFields.add(scannedRow.getObject(scannedIndex++));
            }
        }
        return new FDBTuple(Tuple.fromList(flattenedFields));
    }

    private List<Object> flattenKeys() throws RelationalException {
        List<Object> keys = new ArrayList<>();
        Stack<KeyExpression> nextExpressions = new Stack<>();
        nextExpressions.push(key);
        while (!nextExpressions.isEmpty()) {
            KeyExpression expr = nextExpressions.pop();
            if (expr instanceof FieldKeyExpression) {
                keys.add(((FieldKeyExpression) expr).getFieldName());
            } else if (expr instanceof ThenKeyExpression) {
                List<KeyExpression> children = ((ThenKeyExpression) expr).getChildren();
                for (int i = children.size() - 1; i >= 0; i--) {
                    nextExpressions.push(children.get(i));
                }
            } else if (expr instanceof KeyWithValueExpression ||
                    expr instanceof GroupingKeyExpression ||
                    expr instanceof NestingKeyExpression) {
                List<KeyExpression> children = expr.normalizeKeyForPositions();
                int size = expr instanceof KeyWithValueExpression ? ((KeyWithValueExpression) expr).getSplitPoint() : children.size();
                for (int i = size - 1; i >= 0; i--) {
                    nextExpressions.push(children.get(i));
                }
            } else if (expr instanceof RecordTypeKeyExpression) {
                try {
                    keys.add(typeForKey);
                } catch (RecordCoreException ex) {
                    throw ExceptionUtil.toRelationalException(ex);
                }
            }
        }
        return keys;
    }
}
