/*
 * DimensionsKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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
    private final int prefixSize;
    private final int dimensionsSize;

    private DimensionsKeyExpression(@Nonnull final KeyExpression wholeKey,
                                   final int prefixSize,
                                   final int dimensionsSize) {
        this.wholeKey = wholeKey;
        this.prefixSize = prefixSize;
        this.dimensionsSize = dimensionsSize;
    }

    DimensionsKeyExpression(@Nonnull final RecordMetaDataProto.Dimensions dimensions) throws DeserializationException {
        this(KeyExpression.fromProto(dimensions.getWholeKey()), dimensions.getPrefixSize(), dimensions.getDimensionsSize());
    }

    public int getPrefixSize() {
        return prefixSize;
    }

    public int getDimensionsSize() {
        return dimensionsSize;
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
        if (prefixSize + dimensionsSize > wholeKey.getColumnSize()) {
            throw new InvalidExpressionException("dimensions declared a prefix size and number of dimensions " +
                                                 "that are together larger than the number of columns in the index");
        }
        final List<Descriptors.FieldDescriptor> fieldDescriptors = getWholeKey().validate(descriptor);
        for (int i = prefixSize; i < prefixSize + dimensionsSize; i ++) {
            final Descriptors.FieldDescriptor fieldDescriptor = Objects.requireNonNull(fieldDescriptors.get(i));
            if (fieldDescriptor.getType() != Descriptors.FieldDescriptor.Type.INT64) {
                throw new InvalidExpressionException("the declared dimension columns have to be of type INT64");
            }
        }
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
    public RecordMetaDataProto.Dimensions toProto() throws SerializationException {
        final RecordMetaDataProto.Dimensions.Builder builder = RecordMetaDataProto.Dimensions.newBuilder();
        builder.setWholeKey(getWholeKey().toKeyExpression());
        builder.setPrefixSize(prefixSize);
        builder.setDimensionsSize(dimensionsSize);
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
        if (prefixSize == 0) {
            return null;
        }
        return getWholeKey().getSubKey(0, prefixSize);
    }

    @Nonnull
    public KeyExpression getDimensionsSubKey() {
        return getWholeKey().getSubKey(prefixSize, dimensionsSize);
    }

    @Nonnull
    public KeyExpression getPrefixAndDimensionsKeyExpression() {
        return getWholeKey().getSubKey(0, prefixSize + dimensionsSize);
    }

    @Override
    public String toString() {
        final int restSize = getColumnSize() - prefixSize - dimensionsSize;
        return getWholeKey() +
               " dimensions(" + prefixSize + ", " + dimensionsSize + (restSize > 0 ? ", " + restSize : "") + ")";
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
               (this.prefixSize == that.prefixSize) &&
               (this.dimensionsSize == that.dimensionsSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wholeKey, prefixSize, dimensionsSize);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getWholeKey(), prefixSize, dimensionsSize);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, getWholeKey(), prefixSize, dimensionsSize);
    }

    @Nonnull
    public static DimensionsKeyExpression of(@Nullable final KeyExpression prefix,
                                             @Nonnull final KeyExpression dimensions) {
        final int prefixCount = prefix == null ? 0 : prefix.getColumnSize();
        final int dimensionsCount = dimensions.getColumnSize();
        Verify.verify(dimensionsCount > 1);

        final ImmutableList.Builder<KeyExpression> wholeKeyBuilder = ImmutableList.builder();
        wholeKeyBuilder.addAll(liftExpression(prefix));
        wholeKeyBuilder.addAll(liftExpression(dimensions));

        final KeyExpression wholeKey = Key.Expressions.concat(wholeKeyBuilder.build());

        return new DimensionsKeyExpression(wholeKey, prefixCount, dimensionsCount);
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
