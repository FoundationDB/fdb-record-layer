/*
 * SlidingWindowKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A key expression that wraps a whole key expression with a sliding window constraint.
 * The index maintains only the top-N records based on a globally-scoped window key.
 * The window size and order are specified via index options, not in this expression.
 */
@API(API.Status.EXPERIMENTAL)
public class SlidingWindowKeyExpression extends BaseKeyExpression implements KeyExpressionWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sliding-Window-Key-Expression");

    @Nonnull
    private final KeyExpression wholeKey;
    @Nonnull
    private final KeyExpression windowKey;

    public SlidingWindowKeyExpression(@Nonnull final KeyExpression wholeKey,
                                      @Nonnull final KeyExpression windowKey) {
        this.wholeKey = wholeKey;
        this.windowKey = windowKey;
    }

    SlidingWindowKeyExpression(@Nonnull final RecordKeyExpressionProto.SlidingWindow slidingWindow) throws DeserializationException {
        this(KeyExpression.fromProto(slidingWindow.getWholeKey()),
                KeyExpression.fromProto(slidingWindow.getWindowKey()));
    }

    @Nonnull
    public KeyExpression getWholeKey() {
        return wholeKey;
    }

    @Nonnull
    public KeyExpression getWindowKey() {
        return windowKey;
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable final FDBRecord<M> record, @Nullable final Message message) {
        return getWholeKey().evaluateMessage(record, message);
    }

    @Override
    public boolean createsDuplicates() {
        return getWholeKey().createsDuplicates();
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        final List<Descriptors.FieldDescriptor> fieldDescriptors = getWholeKey().validate(descriptor);
        windowKey.validate(descriptor);
        return fieldDescriptors;
    }

    @Override
    public int getColumnSize() {
        return getWholeKey().getColumnSize();
    }

    @Override
    public boolean needsCopyingToPartialRecord() {
        return getWholeKey().needsCopyingToPartialRecord();
    }

    @Nonnull
    @Override
    public RecordKeyExpressionProto.SlidingWindow toProto() throws SerializationException {
        final RecordKeyExpressionProto.SlidingWindow.Builder builder = RecordKeyExpressionProto.SlidingWindow.newBuilder();
        builder.setWholeKey(getWholeKey().toKeyExpression());
        builder.setWindowKey(getWindowKey().toKeyExpression());
        return builder.build();
    }

    @Nonnull
    @Override
    public RecordKeyExpressionProto.KeyExpression toKeyExpression() {
        return RecordKeyExpressionProto.KeyExpression.newBuilder().setSlidingWindow(toProto()).build();
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        return getWholeKey().normalizeKeyForPositions();
    }

    @Override
    public boolean hasLosslessNormalization() {
        return getWholeKey().hasLosslessNormalization();
    }

    @Nonnull
    @Override
    protected KeyExpression getSubKeyImpl(final int start, final int end) {
        return getWholeKey().getSubKey(start, end);
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull final KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }

    @Override
    public int versionColumns() {
        return getWholeKey().versionColumns();
    }

    @Override
    public boolean hasRecordTypeKey() {
        return getWholeKey().hasRecordTypeKey();
    }

    @Override
    @Nonnull
    public KeyExpression getChild() {
        return getWholeKey();
    }

    @Override
    public String toString() {
        return getWholeKey() + " sliding_window(" + windowKey + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlidingWindowKeyExpression that = (SlidingWindowKeyExpression) o;
        return this.getWholeKey().equals(that.getWholeKey()) &&
               this.getWindowKey().equals(that.getWindowKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(wholeKey, windowKey);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getWholeKey(), getWindowKey());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }
}
