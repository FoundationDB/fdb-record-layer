/*
 * MergeValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class MergeValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Merge-Value");

    @Nonnull
    private final List<? extends QuantifiedColumnValue> children;

    public MergeValue(@Nonnull Iterable<? extends QuantifiedColumnValue> values) {
        this.children = ImmutableList.copyOf(values);
        Preconditions.checkArgument(!children.isEmpty());
    }

    @Nonnull
    @Override
    public Iterable<? extends QuantifiedColumnValue> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public MergeValue withChildren(final Iterable<? extends Value> newChildren) {
        return new MergeValue(StreamSupport.stream(newChildren.spliterator(), false)
                .map(child -> {
                    if (child instanceof QuantifiedColumnValue) {
                        return (QuantifiedColumnValue)child;
                    }
                    throw new RecordCoreException("invalid call to withChildren; expected quantified value");
                })
                .collect(ImmutableList.toImmutableList()));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        if (message == null) {
            return null;
        }

        // TODO For now we'll just go through all the quantifiers and see if they have been bound by the caller.
        //      If they are set, we happily use their value in the merge. If they are unset, we just skip that reference.
        //      This can happen in the context of a set operation as the cursors over the e.g. union may only be
        //      valid for one quantifier (or a subset). Us going through all possible references here will lead to
        //      sub optimal performance as the caller already knows which quantifiers are bound and which ones are not.
        //      Suggestion (although not implemented): We can allow the context to tell us which quantifiers are bound
        //      and which ones are not. EvaluationContext#getBindings() almost does that job except that it also returns
        //      all purely correlated bindings and not just bindings from the quantifiers this set expression ranges
        //      over.
        final ImmutableList<Message> childrenResults =
                children.stream()
                        .flatMap(child -> {
                            final Object childResult = child.eval(store, context, record, message);
                            if (!(childResult instanceof Message)) {
                                return Stream.of();
                            }
                            return Stream.of((Message)childResult);
                        }).collect(ImmutableList.toImmutableList());

        return merge(childrenResults);
    }

    @Nullable
    @SuppressWarnings("unused")
    private Message merge(@Nonnull final List<Message> childrenResults) {
        return null; // TODO do it
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, children);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, children);
    }

    @Override
    public String toString() {
        return "merge(" + children.stream()
                .map(Value::toString)
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    public static Value pivotAndMergeValues(@Nonnull final List<? extends Quantifier> quantifiers) {
        Verify.verify(!quantifiers.isEmpty());
        var numberOfFields = -1;
        final ImmutableList.Builder<List<? extends QuantifiedColumnValue>> allFlowedValuesBuilder = ImmutableList.builder();
        for (final var quantifier : quantifiers) {
            //
            // The following is a shim for the old planner which does not flow type information.
            //
            if (quantifier.getFlowedObjectType().getTypeCode() == Type.TypeCode.UNKNOWN || quantifier.getFlowedObjectType().getTypeCode() == Type.TypeCode.ANY) {
                return quantifier.getFlowedObjectValue();
            }
            
            final var flowedValues = quantifier.getFlowedValues();
            allFlowedValuesBuilder.add(flowedValues);
            if (numberOfFields == -1 || numberOfFields > flowedValues.size()) {
                numberOfFields = flowedValues.size();
            }
        }

        final var allFlowedValues = allFlowedValuesBuilder.build();
        final var mergeValuesBuilder = ImmutableList.<MergeValue>builder();

        for (int i = 0; i < numberOfFields; i ++) {
            final var toBeMergedValuesBuilder = ImmutableList.<QuantifiedColumnValue>builder();
            for (final List<? extends QuantifiedColumnValue> allFlowedValuesFromQuantifier : allFlowedValues) {
                final var quantifiedColumnValue = allFlowedValuesFromQuantifier.get(i);
                toBeMergedValuesBuilder.add(quantifiedColumnValue);
            }
            mergeValuesBuilder.add(new MergeValue(toBeMergedValuesBuilder.build()));
        }

        return RecordConstructorValue.ofUnnamed(mergeValuesBuilder.build());
    }
}
