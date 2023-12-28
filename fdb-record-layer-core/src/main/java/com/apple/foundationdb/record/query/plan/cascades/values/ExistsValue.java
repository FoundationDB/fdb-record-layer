/*
 * ExistsValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.RecordQueryPlanProto;
import com.apple.foundationdb.record.RecordQueryPlanProto.PExistsValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.serialization.ProtoMessage;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link Value} that checks whether an item exists in its child quantifier expression or not.
 */
@API(API.Status.EXPERIMENTAL)
@AutoService(PlanSerializable.class)
@ProtoMessage(PExistsValue.class)
public class ExistsValue extends AbstractValue implements BooleanValue, ValueWithChild, Value.CompileTimeValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Exists-Value");
    @Nonnull
    private final QuantifiedObjectValue child;

    public ExistsValue(@Nonnull QuantifiedObjectValue child) {
        this.child = child;
    }

    @Override
    @SuppressWarnings({"java:S2637", "ConstantConditions"}) // TODO the alternative component should not be null
    @SpotBugsSuppressWarnings("NP_NONNULL_PARAM_VIOLATION")
    public Optional<QueryPredicate> toQueryPredicate(@Nullable final TypeRepository typeRepository,
                                                     @Nonnull final CorrelationIdentifier innermostAlias) {
        return Optional.of(new ExistsPredicate(child.getAlias()));
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    public ValueWithChild withNewChild(@Nonnull final Value newChild) {
        Verify.verify(newChild instanceof QuantifiedObjectValue);
        return new ExistsValue((QuantifiedObjectValue)newChild);
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
        return "exists(" + child.explain(formatter) + ")";
    }

    @Override
    public String toString() {
        return "exists(" + child + ")";
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

    @Nonnull
    @Override
    public PExistsValue toProto(@Nonnull final PlanHashMode mode) {
        return PExistsValue.newBuilder()
                .setChild(child.toProto(mode))
                .build();
    }

    @Nonnull
    @Override
    public RecordQueryPlanProto.PValue toValueProto(@Nonnull final PlanHashMode mode) {
        return RecordQueryPlanProto.PValue.newBuilder().setExistsValue(toProto(mode)).build();
    }

    @Nonnull
    public static ExistsValue fromProto(@Nonnull final PlanHashMode mode, @Nonnull final PExistsValue existsValueProto) {
        return new ExistsValue(QuantifiedObjectValue.fromProto(mode, Objects.requireNonNull(existsValueProto.getChild())));
    }

    /**
     * A function that checks whether an item exists in a {@link RelationalExpression}.
     */
    @AutoService(BuiltInFunction.class)
    public static class ExistsFn extends BuiltInFunction<Value> {
        public ExistsFn() {
            super("exists",
                    ImmutableList.of(new Type.Relation()), (builtInFunction, arguments) -> encapsulateInternal(arguments));
        }

        private static Value encapsulateInternal(@Nonnull final List<? extends Typed> arguments) {
            // the call is already validated against the resolved function
            Verify.verify(arguments.size() == 1);
            final Typed in = arguments.get(0);
            Verify.verify(in instanceof RelationalExpression);

            // create an existential quantifier
            final Quantifier.Existential existsQuantifier = Quantifier.existential(GroupExpressionRef.of((RelationalExpression)in));

            return new ExistsValue(existsQuantifier.getFlowedObjectValue());
        }
    }
}
