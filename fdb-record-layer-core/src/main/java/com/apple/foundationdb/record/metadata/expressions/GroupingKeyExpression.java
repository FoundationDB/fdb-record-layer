/*
 * GroupingKeyExpression.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.ExpansionVisitor;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionVisitor;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A key expression that divides into two parts for the sake of aggregate or rank indexing.
 * Zero or more <i>grouping</i> columns determine a subindex within which an aggregate or ranking is maintained.
 * The remaining (up to <code>Index.getColumnSize()</code>) <i>grouped</i> columns are the value to be aggregated / ranked.
 */
@API(API.Status.MAINTAINED)
public class GroupingKeyExpression extends BaseKeyExpression implements KeyExpressionWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Grouping-Key-Expression");

    @Nonnull
    private final KeyExpression wholeKey;
    private final int groupedCount;

    public GroupingKeyExpression(@Nonnull KeyExpression wholeKey, int groupedCount) {
        this.wholeKey = wholeKey;
        this.groupedCount = groupedCount;
    }

    public GroupingKeyExpression(@Nonnull RecordMetaDataProto.Grouping grouping) throws DeserializationException {
        this(KeyExpression.fromProto(grouping.getWholeKey()), grouping.getGroupedCount());
    }

    public static GroupingKeyExpression of(@Nonnull KeyExpression groupedValue, @Nonnull KeyExpression groupByFirst, @Nonnull KeyExpression... groupByRest) {
        KeyExpression wholeKeyFirst = groupByFirst;
        KeyExpression wholeKeySecond;
        KeyExpression[] wholeKeyRest = new KeyExpression[groupByRest.length];
        if (wholeKeyRest.length == 0) {
            wholeKeySecond = groupedValue;
        } else {
            wholeKeySecond = groupByRest[0];
            System.arraycopy(groupByRest, 1, wholeKeyRest, 0, groupByRest.length - 1);
            wholeKeyRest[wholeKeyRest.length - 1] = groupedValue;
        }
        return new GroupingKeyExpression(Key.Expressions.concat(wholeKeyFirst, wholeKeySecond, wholeKeyRest),
                groupedValue.getColumnSize());
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        return getWholeKey().evaluateMessage(record, message);
    }

    @Override
    public boolean createsDuplicates() {
        return getWholeKey().createsDuplicates();
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        return getWholeKey().validate(descriptor);
    }

    @Override
    public int getColumnSize() {
        return getWholeKey().getColumnSize();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.Grouping toProto() throws SerializationException {
        final RecordMetaDataProto.Grouping.Builder builder = RecordMetaDataProto.Grouping.newBuilder();
        builder.setWholeKey(getWholeKey().toKeyExpression());
        builder.setGroupedCount(groupedCount);
        return builder.build();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setGrouping(toProto()).build();
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        return getWholeKey().normalizeKeyForPositions();
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State> GraphExpansion expand(@Nonnull final ExpansionVisitor<S> visitor) {
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

    @Nonnull
    public KeyExpression getWholeKey() {
        return wholeKey;
    }

    @Override
    @Nonnull
    public KeyExpression getChild() {
        return getGroupingSubKey();
    }

    public int getGroupedCount() {
        return groupedCount;
    }

    /**
     * Get number of leading columns that select the group (e.g., ranked set or atomic aggregate);
     * remaining fields are the value (e.g., score) within the set.
     * @return the number of leading columns that select the group
     * @see #getGroupedCount()
     */
    public int getGroupingCount() {
        return getColumnSize() - groupedCount;
    }

    @Nonnull
    public KeyExpression getGroupedSubKey() {
        return getWholeKey().getSubKey(getGroupingCount(), getColumnSize());
    }

    @Nonnull
    public KeyExpression getGroupingSubKey() {
        return getWholeKey().getSubKey(0, getGroupingCount());
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getWholeKey().toString());
        str.append(" group ").append(groupedCount);
        return str.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupingKeyExpression that = (GroupingKeyExpression)o;
        return this.getWholeKey().equals(that.getWholeKey()) && (this.groupedCount == that.groupedCount);
    }

    @Override
    public int hashCode() {
        int hash = getWholeKey().hashCode();
        hash += groupedCount;
        return hash;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getWholeKey().planHash(hashKind) + groupedCount;
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getWholeKey(), groupedCount);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }

    }
}
