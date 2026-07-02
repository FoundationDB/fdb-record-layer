/*
 * ReplaceValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths;
import static com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan.computeTrieForFieldPaths;

/**
 * A replacement value replaces the underlying child value according to a given set of transformations.
 */
public class ReplaceValue extends AbstractValue {

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Replace-Value");

    @Nonnull
    private final Value child;

    @Nonnull
    private final Map<FieldValue.FieldPath, Value> transformMap;

    @Nonnull
    private final Supplier<Iterable<? extends Value>> childrenSupplier;

    @Nonnull
    private final MessageHelpers.TransformationTrieNode transformationsTrie;

    public ReplaceValue(@Nonnull final Value child,
                        @Nonnull final Map<FieldValue.FieldPath, Value> transformMap) {
        this.child = child;
        this.transformMap = ImmutableMap.copyOf(transformMap);
        this.childrenSupplier = Suppliers.memoize(() -> ImmutableList.<Value>builder().add(child).addAll(transformMap.values()).build());
        this.transformationsTrie = computeTrieForFieldPaths(checkAndPrepareOrderedFieldPaths(transformMap), transformMap);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, child);
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var inRecord = (M)Preconditions.checkNotNull(child.eval(store, context));
        return MessageHelpers.transformMessage(store,
                context,
                transformationsTrie,
                null, // source and destination are always of the same type
                child.getResultType(),
                inRecord.getDescriptorForType(),
                child.getResultType(),
                inRecord.getDescriptorForType(),
                inRecord);
    }

    public String toString() {
        final var str = new StringBuilder("Replace(");
        str.append("[").append(transformMap.keySet().stream().map(FieldValue.FieldPath::toString).collect(Collectors.joining(", "))).append("] ");
        return str.toString();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(transformMap);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return childrenSupplier.get();
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return child.getResultType();
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) > 0);
        final var iterator = newChildren.iterator();
        final ImmutableMap.Builder<FieldValue.FieldPath, Value> newTransformMap = ImmutableMap.builder();
        for (final var entry : transformMap.entrySet()) {
            newTransformMap.put(entry.getKey(), iterator.next());
        }
        return new ReplaceValue(child, newTransformMap.build());
    }
}
