/*
 * NotValue.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
 * A value that flips the output of its boolean child.
 */
@API(API.Status.EXPERIMENTAL)
public class NotValue extends AbstractValue implements BooleanValue {
    /**
     * The hash value of this expression.
     */
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Not-Value");

    /**
     * The child expression.
     */
    @Nonnull
    private final Value child;

    /**
     * Constructs a new {@link NotValue} instance.
     * @param child The child expression.
     */
    public NotValue(@Nonnull Value child) {
        this.child = child;
    }

    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nullable final TypeRepository typeRepository,
                                                     @Nonnull final CorrelationIdentifier innermostAlias) {
        Verify.verify(child instanceof BooleanValue);
        final Optional<QueryPredicate> predicateOptional = ((BooleanValue)child).toQueryPredicate(typeRepository, innermostAlias);
        if (predicateOptional.isPresent()) {
            QueryPredicate queryPredicate = predicateOptional.get();
            if (queryPredicate.equals(ConstantPredicate.FALSE)) {
                return Optional.of(ConstantPredicate.TRUE);
            }
            if (queryPredicate.equals(ConstantPredicate.TRUE)) {
                return Optional.of(ConstantPredicate.FALSE);
            }
            if (queryPredicate.equals(ConstantPredicate.NULL)) {
                return Optional.of(ConstantPredicate.NULL);
            }
            return Optional.of(NotPredicate.not(queryPredicate));
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return ImmutableList.of(child);
    }

    @Nonnull
    @Override
    public NotValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        return new NotValue(Iterables.get(newChildren, 0));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final Object result = child.eval(store, context);
        if (result == null) {
            return null;
        }
        return !(Boolean)result;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, child);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "not(" + child.explain(formatter) + ")";
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

    /**
     * The {@code not} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class NotFn extends BuiltInFunction<Value> {
        public NotFn() {
            super("not",
                    ImmutableList.of(Type.primitiveType(Type.TypeCode.BOOLEAN)),
                    (builtInFunction, arguments) -> encapsulateInternal(arguments));
        }

        private static Value encapsulateInternal(@Nonnull final List<? extends Typed> arguments) {
            return new NotValue((Value)arguments.get(0));
        }
    }
}
