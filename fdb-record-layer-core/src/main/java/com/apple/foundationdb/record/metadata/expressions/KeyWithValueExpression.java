/*
 * KeyWithValueExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A <code>KeyWithValue</code> expression is a top level expression that takes as input an expression that
 * produces more than one column, and indicates that all columns before the specified <code>splitPoint</code>
 * should be used in the key of a covering index and all of the values starting at the <code>splitPoint</code>
 * are to be used as the value of a covering index.
 */
@API(API.Status.MAINTAINED)
public class KeyWithValueExpression extends BaseKeyExpression implements KeyExpressionWithChild {
    @Nonnull
    private final KeyExpression innerKey;
    @Nullable
    private final int splitPoint;

    @Nullable
    private List<KeyExpression> normalizedKeys; // Cached normalized keys
    @Nullable
    private KeyExpression keyExpression; // Cached key
    @Nullable
    private KeyExpression valueExpression; // Cached value

    public KeyWithValueExpression(@Nonnull KeyExpression innerKey, int splitPoint) {
        this.innerKey = innerKey;
        this.splitPoint = splitPoint;
    }

    public KeyWithValueExpression(@Nonnull RecordMetaDataProto.KeyWithValue proto) throws DeserializationException {
        this(KeyExpression.fromProto(proto.getInnerKey()), proto.getSplitPoint());
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        return getInnerKey().evaluateMessage(record, message);
    }

    @Override
    public boolean createsDuplicates() {
        return getInnerKey().createsDuplicates();
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        KeyExpression key = getInnerKey();
        if (key.getColumnSize() < splitPoint) {
            throw new InvalidExpressionException("Child expression of covering expression returns too few columns")
                    .addLogInfo(
                            "split_point", splitPoint,
                            "child_columns", key.getColumnSize());
        }
        return key.validate(descriptor);
    }

    @Override
    public int getColumnSize() {
        // Because this expression is purely used to feed values to an index, the column size is really
        // the size of the values in the key portion of the index.
        return getSplitPoint();
    }

    @Nonnull
    @Override
    protected KeyExpression getSubKeyImpl(int start, int end) {
        return getInnerKey().getSubKey(start, end);
    }

    @Nonnull
    public KeyExpression getKeyExpression() {
        if (keyExpression == null) {
            keyExpression = getInnerKey().getSubKey(0, splitPoint);
        }
        return keyExpression;
    }

    @Nonnull
    public KeyExpression getValueExpression() {
        if (valueExpression == null) {
            List<KeyExpression> allKeys = normalizeKeyForPositions();
            if (splitPoint == allKeys.size()) {
                valueExpression = EmptyKeyExpression.EMPTY;
            } else {
                valueExpression = new ThenKeyExpression(allKeys, splitPoint, allKeys.size());
            }
        }
        return valueExpression;
    }

    public int getChildColumnSize() {
        return getInnerKey().getColumnSize();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyWithValue toProto() throws SerializationException {
        return RecordMetaDataProto.KeyWithValue.newBuilder()
                .setInnerKey(getInnerKey().toKeyExpression())
                .setSplitPoint(splitPoint)
                .build();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setKeyWithValue(toProto()).build();
    }

    @Nonnull
    @Override
    public KeyExpression normalizeForPlanner(@Nonnull Source source, @Nonnull List<String> fieldNamePrefix) {
        return new KeyWithValueExpression(innerKey.normalizeForPlanner(source, fieldNamePrefix), splitPoint);
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        if (normalizedKeys == null) {
            normalizedKeys = getInnerKey().normalizeKeyForPositions();
        }
        return normalizedKeys;
    }

    @Override
    public int versionColumns() {
        return getInnerKey().versionColumns();
    }

    @Override
    public boolean hasRecordTypeKey() {
        return getInnerKey().hasRecordTypeKey();
    }

    public int getSplitPoint() {
        return splitPoint;
    }

    @Nonnull
    private KeyExpression getInnerKey() {
        return innerKey;
    }

    @Nonnull
    @Override
    public KeyExpression getChild() {
        return getKeyExpression();
    }

    /**
     * Given a <code>Key.Evaluated</code> returned from the evaluation of this expression, splits out
     * the portion of that expression that should be used to represent the key of a covering index.
     * @param wholeKey the whole key with both key and value
     * @return the key portion of the given key
     */
    @Nonnull
    public Key.Evaluated getKey(@Nonnull Key.Evaluated wholeKey) {
        return wholeKey.subKey(0, splitPoint);
    }

    /**
     * Given a <code>Key.Evaluated</code> returned from the evaluation of this expression, splits out
     * the portion of that expression that should be used to represent the value of a covering index.
     * @param wholeKey the whole key with both key and value
     * @return the value portion of the given key
     */
    @Nonnull
    public Key.Evaluated getValue(@Nonnull Key.Evaluated wholeKey) {
        return wholeKey.subKey(splitPoint, wholeKey.size());
    }

    @Override
    public String toString() {
        return "covering(" + getInnerKey().toString() + " split " + splitPoint + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KeyWithValueExpression that = (KeyWithValueExpression)o;
        return this.getInnerKey().equals(that.getInnerKey()) && (this.splitPoint == that.splitPoint);
    }

    @Override
    public int hashCode() {
        int result = getInnerKey().hashCode();
        result = 31 * result + splitPoint;
        return result;
    }

    @Override
    public int planHash() {
        return getInnerKey().planHash() + splitPoint;
    }

    @Override
    public List<KeyExpression> getKeyFields() {
       return new ArrayList<>(normalizedKeys.subList(0, getSplitPoint()));
    }

    @Override
    public List<KeyExpression> getValueFields() {
        return new ArrayList<>(normalizedKeys.subList(getSplitPoint(), normalizedKeys.size()));
    }
}
