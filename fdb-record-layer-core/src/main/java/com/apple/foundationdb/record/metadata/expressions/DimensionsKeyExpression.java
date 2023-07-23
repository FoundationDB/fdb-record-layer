/*
 * DimensionsKeyExpression.java
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
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A key expression that divides into three parts that represent the constituent parts of a multidimensional index.
 * Zero or more <em>prefix</em> columns determine a range within which an R-tree structure is maintained.
 * The <em>dimensions</em> columns determine a set of expressions that span an multidimensional canvas. All
 * such columns must be of a numeric type.
 * The remaining (up to <code>Index.getColumnSize()</code>) <em>rest</em> are stored within the R-tree as key-rest or
 * value.
 */
@API(API.Status.MAINTAINED)
public class DimensionsKeyExpression extends BaseKeyExpression implements KeyExpressionWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Dimension-Key-Expression");

    @Nonnull
    private final KeyExpression wholeKey;
    private final int prefixCount;
    private final int dimensionsCount;


    public DimensionsKeyExpression(@Nonnull final KeyExpression wholeKey,
                                   final int prefixCount,
                                   final int dimensionsCount) {
        this.wholeKey = wholeKey;
        this.prefixCount = prefixCount;
        this.dimensionsCount = dimensionsCount;
    }

    public DimensionsKeyExpression(@Nonnull RecordMetaDataProto.Dimensions dimensions) throws DeserializationException {
        this(KeyExpression.fromProto(dimensions.getWholeKey()), dimensions.getPrefixCount(), dimensions.getDimensionsCount());
    }

    public int getPrefixCount() {
        return prefixCount;
    }

    public int getDimensionsCount() {
        return dimensionsCount;
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
        return getWholeKey().validate(descriptor);
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
    public RecordMetaDataProto.Dimensions toProto() throws SerializationException {
        final RecordMetaDataProto.Dimensions.Builder builder = RecordMetaDataProto.Dimensions.newBuilder();
        builder.setWholeKey(getWholeKey().toKeyExpression());
        builder.setPrefixCount(prefixCount);
        builder.setDimensionsCount(dimensionsCount);
        return builder.build();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setDimensions(toProto()).build();
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        return getWholeKey().normalizeKeyForPositions();
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

    @Nonnull
    public KeyExpression getWholeKey() {
        return wholeKey;
    }

    @Override
    @Nonnull
    public KeyExpression getChild() {
        return getWholeKey();
    }

    @Nullable
    public KeyExpression getPrefixSubKey() {
        if (prefixCount == 0) {
            return null;
        }
        return getWholeKey().getSubKey(0, prefixCount);
    }

    @Nonnull
    public KeyExpression getDimensionsSubKey() {
        return getWholeKey().getSubKey(prefixCount, dimensionsCount);
    }

    @Override
    public String toString() {
        return getWholeKey() +
               " dimensions(" + prefixCount + ", " + dimensionsCount + ")" + prefixCount;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DimensionsKeyExpression that = (DimensionsKeyExpression)o;
        return this.getWholeKey().equals(that.getWholeKey()) &&
               (this.prefixCount == that.prefixCount) &&
               (this.dimensionsCount == that.dimensionsCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wholeKey, prefixCount, dimensionsCount);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getWholeKey(), prefixCount, dimensionsCount);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, getWholeKey(), prefixCount, dimensionsCount);
    }

    @Nonnull
    public static DimensionsKeyExpression of(@Nullable final KeyExpression prefix,
                                             @Nonnull final KeyExpression dimensions,
                                             @Nullable final KeyExpression rest) {
        final int prefixCount = prefix == null ? 0 : prefix.getColumnSize();
        final int dimensionsCount = dimensions.getColumnSize();
        Verify.verify(dimensionsCount > 1);

        final ImmutableList.Builder<KeyExpression> wholeKeyBuilder = ImmutableList.builder();
        wholeKeyBuilder.addAll(liftExpression(prefix));
        wholeKeyBuilder.addAll(liftExpression(dimensions));
        wholeKeyBuilder.addAll(liftExpression(rest));

        final KeyExpression wholeKey = Key.Expressions.concat(wholeKeyBuilder.build());

        return new DimensionsKeyExpression(wholeKey, prefixCount, dimensionsCount);
    }

    @Nonnull
    private static List<KeyExpression> liftExpression(@Nullable final KeyExpression expression) {
        if (expression == null) {
            return ImmutableList.of();
        }
        if (expression instanceof ThenKeyExpression) {
            return ((ThenKeyExpression)expression).getChildren();
        }
        return ImmutableList.of(expression);
    }
}
