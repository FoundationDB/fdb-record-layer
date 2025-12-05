/*
 * NestingKeyExpression.java
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
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A key expression within a nested subrecord.
 * If the parent field is repeated, then the parent field must have fan type <code>FanType.FanOut</code>.
 * In that case, this will return the nested expression evaluated against every subrecord (possibly returning
 * no <code>Key.Evaluated</code>s if the parent field is empty). If the parent field is not repeated and not set,
 * then this will evaluate the nested expression on the <code>null</code> record. This should return the same
 * result as if the field were set to the empty message. If this expression is evaluated on the <code>null</code>
 * record, then it will evaluate the same as if the parent field is unset or empty (depending on the fan type).
 */
@API(API.Status.UNSTABLE)
public class NestingKeyExpression extends BaseKeyExpression implements KeyExpressionWithChild, AtomKeyExpression, GroupableKeyExpression {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Nesting-Key-Expression");

    @Nonnull
    private final FieldKeyExpression parent;
    @Nonnull
    private final KeyExpression child;

    public NestingKeyExpression(@Nonnull FieldKeyExpression parent, @Nonnull KeyExpression child) {
        this.parent = parent;
        this.child = child;
    }

    public NestingKeyExpression(@Nonnull RecordKeyExpressionProto.Nesting nesting) throws DeserializationException {
        if (!nesting.hasParent()) {
            throw new DeserializationException("Serialized Nesting is missing parent");
        }
        parent = new FieldKeyExpression(nesting.getParent());
        child = KeyExpression.fromProto(nesting.getChild());
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        final List<Key.Evaluated> parentKeys = parent.evaluateMessage(record, message);
        List<Key.Evaluated> result = new ArrayList<>();
        // TODO make this more type safe. But those components should always be single messages
        for (Key.Evaluated value : parentKeys) {
            final Message submessage = (Message) value.toList().get(0);
            result.addAll(getChild().evaluateMessage(record, submessage));
        }
        validateColumnCounts(result);
        return result;
    }

    @Override
    public boolean createsDuplicates() {
        return parent.createsDuplicates() || getChild().createsDuplicates();
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        parent.validate(descriptor, true);
        return getChild().validate(parent.getDescriptor(descriptor));
    }

    @Override
    public int getColumnSize() {
        return getChild().getColumnSize();
    }

    @Nonnull
    @Override
    public RecordKeyExpressionProto.Nesting toProto() throws SerializationException {
        final RecordKeyExpressionProto.Nesting.Builder builder = RecordKeyExpressionProto.Nesting.newBuilder();
        builder.setParent(parent.toProto());
        builder.setChild(getChild().toKeyExpression());
        return builder.build();
    }

    @Nonnull
    @Override
    public RecordKeyExpressionProto.KeyExpression toKeyExpression() {
        return RecordKeyExpressionProto.KeyExpression.newBuilder().setNesting(toProto()).build();
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        return getChild().normalizeKeyForPositions()
                .stream().map(normalizedChild -> new NestingKeyExpression(parent, normalizedChild))
                .collect(Collectors.toList());
    }

    @Override
    public boolean hasLosslessNormalization() {
        if (parent.getFanType() == FanType.FanOut && child.getColumnSize() > 1) {
            // Multiple repeated children will be correlated by each parent, whereas the normalized version is a cross-product.
            return false;
        }
        return child.hasLosslessNormalization();
    }

    @Override
    public boolean needsCopyingToPartialRecord() {
        return child.needsCopyingToPartialRecord();
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull final KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }

    @Override
    public int versionColumns() {
        return getChild().versionColumns();
    }

    @Override
    public boolean hasRecordTypeKey() {
        return getChild().hasRecordTypeKey();
    }

    @Override
    public KeyExpression getSubKeyImpl(int start, int end) {
        KeyExpression childKey = getChild().getSubKey(start, end);
        return new NestingKeyExpression(parent, childKey);
    }

    @Nonnull
    public FieldKeyExpression getParent() {
        return parent;
    }

    @Override
    @Nonnull
    public KeyExpression getChild() {
        return child;
    }

    @Nonnull
    @Override
    public GroupingKeyExpression ungrouped() {
        return new GroupingKeyExpression(this, getColumnSize());
    }

    @Nonnull
    @Override
    public GroupingKeyExpression groupBy(@Nonnull KeyExpression groupByFirst, @Nonnull KeyExpression... groupByRest) {
        return GroupingKeyExpression.of(this, groupByFirst, groupByRest);
    }

    @Override
    public String toString() {
        return parent + "/" + getChild();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NestingKeyExpression that = (NestingKeyExpression)o;
        return this.parent.equals(that.parent) && this.getChild().equals(that.getChild());
    }

    @Override
    public int hashCode() {
        return parent.hashCode() + getChild().hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return parent.planHash(mode) + getChild().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH, parent, getChild());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public boolean equalsAtomic(AtomKeyExpression other) {
        return this.getClass() == other.getClass() && parent.equals(((NestingKeyExpression) other).parent);
    }
}
