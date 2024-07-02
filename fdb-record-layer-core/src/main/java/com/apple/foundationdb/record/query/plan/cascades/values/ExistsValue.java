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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PExistsValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
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
public class ExistsValue extends AbstractValue implements BooleanValue, QuantifiedValue, Value.CompileTimeValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Exists-Value");
    @Nonnull
    private final CorrelationIdentifier alias;

    public ExistsValue(@Nonnull CorrelationIdentifier alias) {
        this.alias = alias;
    }

    @Override
    @SuppressWarnings({"java:S2637", "ConstantConditions"}) // TODO the alternative component should not be null
    @SpotBugsSuppressWarnings("NP_NONNULL_PARAM_VIOLATION")
    public Optional<QueryPredicate> toQueryPredicate(@Nullable final TypeRepository typeRepository,
                                                     @Nonnull final CorrelationIdentifier innermostAlias) {
        return Optional.of(new ExistsPredicate(alias));
    }

    @Nonnull
    @Override
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, alias);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "exists(" + alias + ")";
    }

    @Override
    public String toString() {
        return "exists(" + alias + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PExistsValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PExistsValue.newBuilder()
                .setAlias(alias.getId())
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setExistsValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ExistsValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PExistsValue existsValueProto) {
        if (existsValueProto.hasAlias()) {
            return new ExistsValue(CorrelationIdentifier.of(Objects.requireNonNull(existsValueProto.getAlias())));
        }
        // TODO deprecated -- remove this
        return new ExistsValue(QuantifiedObjectValue.fromProto(serializationContext,
                Objects.requireNonNull(existsValueProto.getChild())).getAlias());
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
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
            final Quantifier.Existential existsQuantifier = Quantifier.existential(Reference.of((RelationalExpression)in));

            return new ExistsValue(existsQuantifier.getAlias());
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PExistsValue, ExistsValue> {
        @Nonnull
        @Override
        public Class<PExistsValue> getProtoMessageClass() {
            return PExistsValue.class;
        }

        @Nonnull
        @Override
        public ExistsValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PExistsValue existsValueProto) {
            return ExistsValue.fromProto(serializationContext, existsValueProto);
        }
    }
}
