/*
 * SplitKeyExpression.java
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
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpandedPredicates;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Turn a key with repeated single values into multiple <code>Key.Evaluated</code> containing several of the values.
 * For example, <code>12</code> values with <code>splitSize</code> <code>3</code> turns into <code>4</code> <code>Key.Evaluated</code>.
 * The same result can be achieved more transparently by having a repeated nested submessage with the several fields
 * in it; this is for the sake of clients with poorer type systems that only allow for lists of primitive types.
 */
@API(API.Status.MAINTAINED)
public class SplitKeyExpression extends BaseKeyExpression implements AtomKeyExpression, KeyExpressionWithoutChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Split-Key-Expression");

    private final KeyExpression joined;
    private final int splitSize;

    public SplitKeyExpression(KeyExpression joined, int splitSize) {
        this.joined = joined;
        this.splitSize = splitSize;
    }

    public SplitKeyExpression(@Nonnull RecordMetaDataProto.Split split) throws DeserializationException {
        this(KeyExpression.fromProto(split.getJoined()), split.getSplitSize());
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        return split(getJoined().evaluateMessage(record, message));
    }

    private List<Key.Evaluated> split(@Nonnull List<Key.Evaluated> unsplit) {
        if (unsplit.size() % splitSize != 0) {
            throw new RecordCoreException("stored value size is not an even multiple of " + splitSize);
        }
        final List<Key.Evaluated> split = new ArrayList<>(unsplit.size() / splitSize);
        for (int i = 0; i < unsplit.size(); i += splitSize) {
            Key.Evaluated item = unsplit.get(i);
            for (int j = 1; j < splitSize; j++) {
                item = item.append(unsplit.get(i + j));
            }
            split.add(item);
        }
        validateColumnCounts(split);
        return split;
    }

    @Override
    public boolean createsDuplicates() {
        return true;
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        if (getJoined().getColumnSize() != 1) {
            throw new InvalidExpressionException("Must have a single key before splitting");
        }
        if (!getJoined().createsDuplicates()) {
            throw new InvalidExpressionException("Must produce multiple values for splitting");
        }
        return getJoined().validate(descriptor);
    }

    @Override
    public int getColumnSize() {
        return splitSize;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.Split toProto() throws SerializationException {
        final RecordMetaDataProto.Split.Builder builder = RecordMetaDataProto.Split.newBuilder();
        builder.setJoined(getJoined().toKeyExpression());
        builder.setSplitSize(splitSize);
        return builder.build();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setSplit(toProto()).build();
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        return Collections.nCopies(splitSize, getJoined());
    }

    @Nonnull
    @Override
    public ExpandedPredicates normalizeForPlanner(@Nonnull final CorrelationIdentifier baseAlias,
                                                  @Nonnull final Supplier<CorrelationIdentifier> parameterAliasSupplier,
                                                  @Nonnull final List<String> fieldNamePrefix) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public int versionColumns() {
        return getJoined().versionColumns();
    }

    @Override
    public boolean hasRecordTypeKey() {
        return getJoined().hasRecordTypeKey();
    }

    @Nonnull
    public KeyExpression getJoined() {
        return joined;
    }

    /**
     * Get this entire split as a group without any grouping keys.
     * @return this split without any grouping keys
     */
    @Nonnull
    public GroupingKeyExpression ungrouped() {
        return new GroupingKeyExpression(this, getColumnSize());
    }

    @Nonnull
    public GroupingKeyExpression groupBy(@Nonnull KeyExpression groupByFirst, @Nonnull KeyExpression... groupByRest) {
        return GroupingKeyExpression.of(this, groupByFirst, groupByRest);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getJoined().toString());
        str.append(" split ").append(splitSize);
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

        SplitKeyExpression that = (SplitKeyExpression)o;
        return this.getJoined().equals(that.getJoined()) && (this.splitSize == that.splitSize);
    }

    @Override
    public int hashCode() {
        int hash = getJoined().hashCode();
        hash += splitSize;
        return hash;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getJoined().planHash(hashKind) + splitSize;
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getJoined(), splitSize);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public boolean equalsAtomic(AtomKeyExpression other) {
        return equals(other);
    }
}
