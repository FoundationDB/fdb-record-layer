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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Combine keys from zero or more child keys.
 * Form a cross-product similar to {@link ThenKeyExpression}, but producing lists containing each
 * child's evaluation result rather than concatenating.
 * When converted to a {@link com.apple.foundationdb.tuple.Tuple}, the <i>nth</i> child corresponds
 * to {@code tuple.getNestedTuple(n)}, which is less compact on disk but easier to find the child boundaries in.
 */
@API(API.Status.MAINTAINED)
public class ListKeyExpression extends BaseKeyExpression implements KeyExpressionWithChildren {
    @Nonnull
    private final List<ExpressionRef<KeyExpression>> children;

    public ListKeyExpression(@Nonnull List<KeyExpression> exprs) {
        children = exprs.stream().map(SingleExpressionRef::of).collect(Collectors.toList());
    }

    private ListKeyExpression(@Nonnull ListKeyExpression orig, int start, int end) {
        children = orig.children.subList(start, end);
    }

    public ListKeyExpression(@Nonnull RecordMetaDataProto.List list) throws DeserializationException {
        children = list.getChildList().stream()
                .map(proto -> SingleExpressionRef.of(KeyExpression.fromProto(proto)))
                .collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        final List<List<Key.Evaluated>> childrenValues = new ArrayList<>(children.size());
        int totalCount = 1;
        for (ExpressionRef<KeyExpression> child : children) {
            List<Key.Evaluated> childValues = child.get().evaluateMessage(record, message);
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
        return children.stream().anyMatch(ref -> ref.get().createsDuplicates());
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        return children.stream().flatMap(child -> child.get().validate(descriptor).stream()).collect(Collectors.toList());
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
        for (ExpressionRef<KeyExpression> child : children) {
            builder.addChild(child.get().toKeyExpression());
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
    public List<KeyExpression> normalizeKeyForPositions() {
        return children.stream()
            .map(ExpressionRef::get)
            .collect(Collectors.toList());
    }

    @Override
    public int versionColumns() {
        return children.stream().mapToInt(ref -> ref.get().versionColumns()).sum();
    }

    @Override
    public boolean hasRecordTypeKey() {
        return children.stream().anyMatch(ref -> ref.get().hasRecordTypeKey());
    }

    @Override
    public KeyExpression getSubKeyImpl(int start, int end) {
        return new ListKeyExpression(this, start, end);
    }

    @Nonnull
    @Override
    public List<KeyExpression> getChildren() {
        return children.stream().map(ExpressionRef::get).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return children.iterator();
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
    public int planHash() {
        return PlanHashable.planHash(getChildren());
    }

}
