/*
 * ListKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Combine keys from zero or more child keys.
 *
 * <p>
 * Form a cross-product similar to {@link ThenKeyExpression}, but producing lists containing each
 * child's evaluation result rather than concatenating.
 * When converted to a {@link com.apple.foundationdb.tuple.Tuple}, the <i>nth</i> child corresponds
 * to {@code tuple.getNestedTuple(n)}, which is less compact on disk but easier to find the child boundaries in.
 * </p>
 *
 * <p>
 * Consider the expressions
 * <code>concat(field("child_1").nest(field("field_1")), field("child_2").nest(concat(field("field_1"), field("field_2")))</code> and
 * <code>concat(field("child_1").nest(concat(field("field_1"), field("field_2"))), field("child_2").nest(field("field_1")))</code>.
 * These might produce values like <code>[[1.1, 2.1, 2.2]]</code> and <code>[[1.1, 1.2, 2.1]]</code>, respectively.
 * Recovering the first child's contribution means remembering it or at least its {@link KeyExpression#getColumnSize()}, to know where the boundary is.
 * </p>
 *
 * <p>
 * Contrast
 * <code>list(field("child_1").nest(field("field_1")), field("child_2").nest(concat(field("field_1"), field("field_2")))</code> and
 * <code>list(field("child_1").nest(concat(field("field_1"), field("field_2"))), field("child_2").nest(field("field_1")))</code>.
 * These would produce <code>[[[1.1], [2.1, 2.2]]]</code> and <code>[[[1.1, 1.2], [2.1]]]</code>, respectively.
 * </p>
 */
@API(API.Status.UNSTABLE)
public class ListKeyExpression extends BaseKeyExpression implements KeyExpressionWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("List-Key-Expression");

    @Nonnull
    private final List<KeyExpression> children;

    public ListKeyExpression(@Nonnull List<KeyExpression> exprs) {
        children = exprs;
    }

    private ListKeyExpression(@Nonnull ListKeyExpression orig, int start, int end) {
        children = orig.children.subList(start, end);
    }

    public ListKeyExpression(@Nonnull RecordMetaDataProto.List list) throws DeserializationException {
        children = list.getChildList().stream().map(KeyExpression::fromProto).collect(Collectors.toList());
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
        combine(combined, Collections.emptyList(), 0, childrenValues);
        validateColumnCounts(combined);
        return combined;
    }

    private void combine(@Nonnull List<Key.Evaluated> combined, @Nonnull List<Object> listSoFar, int valuesIndex,
                         @Nonnull List<List<Key.Evaluated>> childrenValues) {
        if (valuesIndex == childrenValues.size()) {
            combined.add(Key.Evaluated.concatenate(listSoFar));
        } else {
            for (Key.Evaluated childValue : childrenValues.get(valuesIndex)) {
                List<Object> nextList = new ArrayList<>(listSoFar.size() + 1);
                nextList.addAll(listSoFar);
                nextList.add(childValue.toTupleAppropriateList());
                combine(combined, nextList, valuesIndex + 1, childrenValues);
            }
        }
    }

    @Override
    public boolean createsDuplicates() {
        return children.stream().anyMatch(KeyExpression::createsDuplicates);
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        return children.stream().flatMap(child -> child.validate(descriptor).stream()).collect(Collectors.toList());
    }

    /**
     * Returns the number of items in each KeyValue that will be returned.
     * Note that this is exactly the number of child expressions, rather than the sum of their sizes,
     * because each one becomes a separate nested tuple.
     * @return the number of elements that will be produced for every key
     */
    @Override
    public int getColumnSize() {
        return children.size();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.List toProto() throws SerializationException {
        final RecordMetaDataProto.List.Builder builder = RecordMetaDataProto.List.newBuilder();
        for (KeyExpression child : children) {
            builder.addChild(child.toKeyExpression());
        }
        return builder.build();
    }

    @Override
    @Nonnull
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setList(toProto()).build();
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull final KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        // The list key expression places each child (regardless of the number of columns) into a
        // single nested tuple when it is evaluated. When normalizing this key expression, we
        // need each child to be placed in its own position, but we wrap it in a list so that
        // we don't lose the fact that the children will be nested. This also maintains the
        // invariant that:
        //    expr.evaluate(record) == Key.Expressions.concat(expr.normalizeKeyForPositions()).evaluate(record)
        if (children.isEmpty()) {
            return ImmutableList.of();
        } else if (children.size() == 1) {
            return ImmutableList.of(this);
        }
        ImmutableList.Builder<KeyExpression> builder = ImmutableList.builderWithExpectedSize(children.size());
        for (KeyExpression child : children) {
            builder.add(Key.Expressions.list(child));
        }
        return builder.build();
    }

    @Override
    public boolean hasLosslessNormalization() {
        for (KeyExpression key : children) {
            if (!key.hasLosslessNormalization()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int versionColumns() {
        return children.stream().mapToInt(KeyExpression::versionColumns).sum();
    }

    @Override
    public boolean hasRecordTypeKey() {
        return children.stream().anyMatch(KeyExpression::hasRecordTypeKey);
    }

    @Override
    public KeyExpression getSubKeyImpl(int start, int end) {
        return new ListKeyExpression(this, start, end);
    }

    @Nonnull
    @Override
    public List<KeyExpression> getChildren() {
        return children;
    }

    @Override
    public boolean needsCopyingToPartialRecord() {
        return getChildren().stream()
                .anyMatch(KeyExpression::needsCopyingToPartialRecord);
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

        ListKeyExpression that = (ListKeyExpression)o;
        return this.getChildren().equals(that.getChildren());
    }

    @Override
    public int hashCode() {
        return getChildren().hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.planHash(mode, getChildren());
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, getChildren());
    }
}
