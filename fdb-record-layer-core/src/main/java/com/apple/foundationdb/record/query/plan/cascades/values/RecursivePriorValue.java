/*
 * RecursivePriorValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecursivePriorValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A value representing the version of a quantifier from the <em>prior</em> iteration of a recursion.
 */
@API(API.Status.EXPERIMENTAL)
public class RecursivePriorValue extends AbstractValue implements LeafValue, PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Recursive-Prior-Value");

    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final Type resultType;

    private RecursivePriorValue(@Nonnull CorrelationIdentifier alias, @Nonnull Type resultType) {
        this.alias = alias;
        this.resultType = resultType;
    }

    @Nonnull
    public static RecursivePriorValue of(@Nonnull CorrelationIdentifier alias, @Nonnull Type resultType) {
        return new RecursivePriorValue(alias, resultType);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return List.of();
    }

    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var binding = (QueryResult)context.getBinding(CorrelationIdentifier.of("prior_" + alias.getId()));
        if (resultType.isRecord()) {
            return binding.getDatum() == null ? null : binding.getMessage();
        } else {
            return binding.getDatum();
        }
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH);
    }

    @Override
    public String toString() {
        return "Prior(" + alias + ")";
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "PRIOR " + alias;
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
    public PRecursivePriorValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecursivePriorValue.newBuilder()
                .setAlias(alias.getId())
                .setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRecursivePriorValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecursivePriorValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PRecursivePriorValue recursivePriorValue) {
        return new RecursivePriorValue(CorrelationIdentifier.of(Objects.requireNonNull(recursivePriorValue.getAlias())),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(recursivePriorValue.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecursivePriorValue, RecursivePriorValue> {
        @Nonnull
        @Override
        public Class<PRecursivePriorValue> getProtoMessageClass() {
            return PRecursivePriorValue.class;
        }

        @Nonnull
        @Override
        public RecursivePriorValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PRecursivePriorValue recursivePriorProto) {
            return RecursivePriorValue.fromProto(serializationContext, recursivePriorProto);
        }
    }
}
