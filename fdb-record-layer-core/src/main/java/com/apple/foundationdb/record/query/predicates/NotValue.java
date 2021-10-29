/*
 * AndOrValue.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class NotValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Not-Value");
    @Nonnull
    private final Value child;

    public NotValue(@Nonnull Value child) {
        this.child = child;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.BOOLEAN);
    }

    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nonnull final CorrelationIdentifier innermostAlias) {
        Verify.verify(child instanceof BooleanValue);
        final Optional<? extends QueryPredicate> predicateOptional = ((BooleanValue)child).toQueryPredicate(innermostAlias);
        return predicateOptional.map(NotPredicate::not);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return ImmutableList.of(child);
    }

    @Nonnull
    @Override
    public NotValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new NotValue(Iterables.get(newChildren, 0));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        final Object result = child.eval(store, context, record, message);
        if (result == null) {
            return false;
        }

        return !(Boolean)result;
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, child);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, child);
    }

    @Override
    public String toString() {
        return "not(" + child + ")";
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

    @AutoService(BuiltInFunction.class)
    public static class NotFn extends BuiltInFunction<Value> {
        public NotFn() {
            super("not",
                    ImmutableList.of(Type.primitiveType(Type.TypeCode.BOOLEAN)), NotFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Atom> arguments) {
            return new NotValue((Value)arguments.get(0));
        }
    }
}
