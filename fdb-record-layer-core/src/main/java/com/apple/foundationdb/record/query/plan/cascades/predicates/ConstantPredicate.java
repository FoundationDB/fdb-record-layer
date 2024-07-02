/*
 * ConstantPredicate.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PConstantPredicate;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * A predicate with a constant boolean value.
 */
@API(API.Status.EXPERIMENTAL)
public class ConstantPredicate extends AbstractQueryPredicate implements LeafQueryPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Constant-Predicate");

    @Nonnull
    public static final ConstantPredicate TRUE = new ConstantPredicate(true);
    @Nonnull
    public static final ConstantPredicate FALSE = new ConstantPredicate(false);
    @Nonnull
    public static final ConstantPredicate NULL = new ConstantPredicate(null);

    @Nullable
    private final Boolean value;

    public ConstantPredicate(@Nonnull final PlanSerializationContext serializationContext,
                             @Nonnull final PConstantPredicate constantPredicateProto) {
        super(serializationContext, Objects.requireNonNull(constantPredicateProto.getSuper()));
        if (constantPredicateProto.hasValue()) {
            this.value = constantPredicateProto.getValue();
        } else {
            this.value = null;
        }
    }

    public ConstantPredicate(@Nullable Boolean value) {
        super(false);
        this.value = value;
    }

    @Override
    public boolean isTautology() {
        return value != null && value;
    }

    @Override
    public boolean isContradiction() {
        return value != null && !value;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return value;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public QueryPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        return this;
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final QueryPredicate other,
                                                       @Nonnull final ValueEquivalence valueEquivalence) {
        return LeafQueryPredicate.super.equalsWithoutChildren(other, valueEquivalence)
                .filter(ignored -> {
                    final ConstantPredicate that = (ConstantPredicate)other;
                    return Objects.equals(value, that.value);
                });
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeSemanticHashCode() {
        return LeafQueryPredicate.super.computeSemanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(value);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, value);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Nonnull
    @Override
    public PConstantPredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PConstantPredicate.newBuilder()
                .setSuper(toAbstractQueryPredicateProto(serializationContext));
        if (value != null) {
            builder.setValue(value);
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setConstantPredicate(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ConstantPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PConstantPredicate constantPredicateProto) {
        return new ConstantPredicate(serializationContext, constantPredicateProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PConstantPredicate, ConstantPredicate> {
        @Nonnull
        @Override
        public Class<PConstantPredicate> getProtoMessageClass() {
            return PConstantPredicate.class;
        }

        @Nonnull
        @Override
        public ConstantPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                           @Nonnull final PConstantPredicate constantPredicateProto) {
            return ConstantPredicate.fromProto(serializationContext, constantPredicateProto);
        }
    }
}
