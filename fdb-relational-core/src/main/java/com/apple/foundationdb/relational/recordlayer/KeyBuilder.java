/*
 * KeyBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;

public class KeyBuilder {
    private final RecordType typeForKey;
    private final KeyExpression key;
    private final String scannableNameForMessage;

    public KeyBuilder(RecordType typeForKey, KeyExpression key, String scannableName) {
        this.key = key;
        this.typeForKey = typeForKey;
        this.scannableNameForMessage = scannableName;
    }

    @Nonnull
    public Row buildKey(Map<String, Object> keyFields, boolean failOnMissingColumn, boolean failOnUnknownColumn) throws RelationalException {
        Object[] flattenedFields = new Object[key.getColumnSize()];
        Map<String, Object> keysNotPicked = new HashMap<>();
        keysNotPicked.putAll(keyFields);
        buildKeyRecursive(key, keyFields, flattenedFields, keysNotPicked, 0);

        if (failOnMissingColumn) {
            for (int i = 0; i < flattenedFields.length; i++) {
                if (flattenedFields[i] == null) {
                    //check to see if this is the tail of the key, in which case this is still fine
                    for (int j = i + 1; j < flattenedFields.length; j++) {
                        if (flattenedFields[j] != null) {
                            //it's not the tail of the key! bad bad bad! eject eject abort!
                            //TODO(bfines) change this to refer to the actual missing column key
                            throw new RelationalException("Cannot form key: missing key at position <" + i + ">", ErrorCode.INVALID_PARAMETER);
                        }
                    }
                    //this is actually ok, we are just scanning on a prefix portion of the full key structure
                    break;
                }
            }
        }

        if (failOnUnknownColumn && !keysNotPicked.isEmpty()) {
            throw new RelationalException("Unknown keys for " + scannableNameForMessage + ", unknown keys: <" + Joiner.on(",").join(keysNotPicked.keySet()) + ">",
                    ErrorCode.INVALID_PARAMETER);
        }
        return new FDBTuple(Tuple.from(flattenedFields));
    }

    private int buildKeyRecursive(KeyExpression expression, Map<String, Object> keyFields, Object[] dest, Map<String, Object> keysNotPicked, int position) throws RelationalException {
        if (position >= dest.length) {
            return position;
        }
        if (expression instanceof FieldKeyExpression) {
            FieldKeyExpression fke = (FieldKeyExpression) expression;
            //TODO(bfines) type validation of parameters?
            String key = fke.getFieldName().toUpperCase(Locale.ROOT);
            dest[position] = keyFields.get(key);
            if (dest[position] != null) {
                keysNotPicked.remove(key);
            }
            return position + 1;
        } else if (expression instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) expression).getChildren();
            for (KeyExpression child : children) {
                position = buildKeyRecursive(child, keyFields, dest, keysNotPicked, position);
            }
            return position + 1;
        } else if (expression instanceof KeyWithValueExpression) {
            KeyWithValueExpression kve = (KeyWithValueExpression) expression;
            List<KeyExpression> normalizedChildren = kve.normalizeKeyForPositions();
            for (KeyExpression child : normalizedChildren) {
                position = buildKeyRecursive(child, keyFields, dest, keysNotPicked, position);
            }
            return position + 1;
        } else if (expression instanceof GroupingKeyExpression) {
            GroupingKeyExpression gke = (GroupingKeyExpression) expression;
            final List<KeyExpression> normalizedChildren = gke.normalizeKeyForPositions();
            for (KeyExpression child : normalizedChildren) {
                position = buildKeyRecursive(child, keyFields, dest, keysNotPicked, position);
            }
            return position + 1;
        } else if (expression instanceof NestingKeyExpression) {
            NestingKeyExpression nke = (NestingKeyExpression) expression;
            final List<KeyExpression> normalizedChildren = nke.normalizeKeyForPositions();
            for (KeyExpression child : normalizedChildren) {
                position = buildKeyRecursive(child, keyFields, dest, keysNotPicked, position);
            }
            return position + 1;
        } else if (expression instanceof RecordTypeKeyExpression) {
            dest[position] = typeForKey.getRecordTypeKey();
            return position + 1;
        } else {
            throw new RelationalException("Unknown Key type: <" + expression.getClass() + ">", ErrorCode.UNKNOWN);
        }
    }

}
