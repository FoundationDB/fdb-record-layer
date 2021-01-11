/*
 * ThenKeyExpression.java
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
import com.apple.foundationdb.record.RecordCoreException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Combine keys from two or more child keys.
 * Form a cross-product of all the <code>Key.Evaluated</code> elements returned by <code>evaluate</code> and
 * then combine the keys within each member of the cross-product. If this is evaluated on the
 * <code>null</code> record, then it returns the concatenation of each of its children evaluated
 * against the <code>null</code> record.
 */
@API(API.Status.MAINTAINED)
public class ThenKeyExpression extends BaseKeyExpression implements KeyExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Then-Key-Expression");

    @Nonnull
    private final List<KeyExpression> children;

    public ThenKeyExpression(@Nonnull List<KeyExpression> exprs) {
        this(exprs, 0, exprs.size());
    }

    public ThenKeyExpression(@Nonnull List<KeyExpression> exprs, int startIdx, int endIdx) {
        children = new ArrayList<>(endIdx - startIdx);
        for (int i = startIdx; i < endIdx; i++) {
            add(children, exprs.get(i));
        }
        if (children.size() < 2) {
            throw new RecordCoreException("Then must have at least 2 children");
        }
    }

    public ThenKeyExpression(@Nonnull KeyExpression first, @Nonnull KeyExpression second, @Nonnull KeyExpression... rest) {
        children = new ArrayList<>(rest.length + 2);
        add(children, first);
        add(children, second);
        for (KeyExpression child : rest) {
            add(children, child);
        }
    }

    public ThenKeyExpression(@Nonnull RecordMetaDataProto.Then then) throws DeserializationException {
        children = new ArrayList<>(then.getChildCount());
        for (RecordMetaDataProto.KeyExpression child : then.getChildList()) {
            final KeyExpression expression = KeyExpression.fromProto(child);
            add(children, expression);
        }
        if (children.size() < 2) {
            throw new DeserializationException("Then must have at least 2 children");
        }
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        final List<List<Key.Evaluated>> childrenValues = new ArrayList<>(children.size());
        int totalCount = 1;
        for (KeyExpression child : children) {
            List<Key.Evaluated> childValues = child.evaluateMessage(record, message);
            childrenValues.add(childValues);
            totalCount *= childValues.size();
        }
        return combine(childrenValues, totalCount);
    }

    private List<Key.Evaluated> combine(@Nonnull List<List<Key.Evaluated>> childrenValues, int totalCount) {
        final List<Key.Evaluated> combined = new ArrayList<>(totalCount);
        for (Key.Evaluated childValue : childrenValues.get(0)) {
            combine(combined, childValue, 1, childrenValues);
        }
        validateColumnCounts(combined);
        return combined;
    }

    private void combine(@Nonnull List<Key.Evaluated> combined, @Nonnull Key.Evaluated prefix, int valuesIndex,
                         @Nonnull List<List<Key.Evaluated>> childrenValues) {
        if (valuesIndex == childrenValues.size()) {
            combined.add(prefix);
        } else {
            for (Key.Evaluated childValue : childrenValues.get(valuesIndex)) {
                combine(combined, prefix.append(childValue), valuesIndex + 1, childrenValues);
            }
        }
    }

    @Override
    public boolean createsDuplicates() {
        return createsDuplicatesAfter(0);
    }

    public boolean createsDuplicatesAfter(int index) {
        for (int i = index; i < children.size(); i++) {
            if (children.get(i).createsDuplicates()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        return children.stream().flatMap(child -> child.validate(descriptor).stream()).collect(Collectors.toList());
    }

    @Override
    public int getColumnSize() {
        int columnSize = 0;
        for (KeyExpression child : children) {
            columnSize += child.getColumnSize();
        }
        return columnSize;
    }

    /**
     * Get this entire concatenation as a group without any grouping keys.
     * @return this concatenation without any grouping keys
     */
    @Nonnull
    public GroupingKeyExpression ungrouped() {
        return new GroupingKeyExpression(this, getColumnSize());
    }

    @Nonnull
    public GroupingKeyExpression groupBy(@Nonnull KeyExpression groupByFirst, @Nonnull KeyExpression... groupByRest) {
        return GroupingKeyExpression.of(this, groupByFirst, groupByRest);
    }

    @Nonnull
    public GroupingKeyExpression group(int count) {
        if (count < 0 || count > getColumnSize()) {
            throw new RecordCoreException("Grouped count out of range");
        }
        return new GroupingKeyExpression(this, count);
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.Then toProto() throws SerializationException {
        final RecordMetaDataProto.Then.Builder builder = RecordMetaDataProto.Then.newBuilder();
        for (KeyExpression child : children) {
            builder.addChild(child.toKeyExpression());
        }
        return builder.build();
    }

    @Override
    @Nonnull
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setThen(toProto()).build();
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State> GraphExpansion expand(@Nonnull final ExpansionVisitor<S> visitor) {
        return visitor.visitExpression(this);
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        return getChildren()
                .stream().flatMap(k -> k.normalizeKeyForPositions().stream())
                .collect(Collectors.toList());
    }

    @Override
    public int versionColumns() {
        int versionColumns = 0;
        for (KeyExpression subkey : getChildren()) {
            versionColumns += subkey.versionColumns();
        }
        return versionColumns;
    }

    @Override
    public boolean hasRecordTypeKey() {
        for (KeyExpression subkey : getChildren()) {
            if (subkey.hasRecordTypeKey()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public KeyExpression getSubKeyImpl(int start, int end) {
        List<KeyExpression> childrenForSubKey = new ArrayList<>(getChildren());
        int childStart = splitGroupingKeys(childrenForSubKey, start);
        int childEnd = splitGroupingKeys(childrenForSubKey, end);
        if (childStart == childEnd) {
            return EmptyKeyExpression.EMPTY;
        } else if (childStart == childEnd - 1) {
            return childrenForSubKey.get(childStart);
        } else {
            return new ThenKeyExpression(childrenForSubKey.subList(childStart, childEnd));
        }
    }

    @Nonnull
    @Override
    public List<KeyExpression> getChildren() {
        return children;
    }

    @Nonnull
    public List<KeyExpression> getChildrenRefs() {
        return children;
    }

    // should not be used outside of the Then constructors
    private static void add(@Nonnull List<KeyExpression> children, @Nonnull KeyExpression child) {
        if (child instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression) child;
            children.addAll(then.getChildrenRefs());
        } else {
            children.add(child);
        }
    }

    @Override
    public String toString() {
        return getChildren().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ThenKeyExpression that = (ThenKeyExpression)o;
        return this.getChildren().equals(that.getChildren());
    }

    @Override
    public int hashCode() {
        return getChildren().hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return PlanHashable.planHash(hashKind, getChildren());
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getChildren());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    private static int splitGroupingKeys(List<KeyExpression> keys, int pos) {
        int i = 0;
        int total = 0;
        while (i < keys.size() && total < pos) {
            KeyExpression key = keys.get(i);
            int count = key.getColumnSize();
            if (total + count <= pos) {
                i++;
                total += count;
            } else {
                int split = pos - total;
                KeyExpression left = key.getSubKey(0, split);
                KeyExpression right = key.getSubKey(split, count);
                keys.set(i++, left);
                keys.add(i, right);
                total += split;
            }
        }
        return i;
    }
}

