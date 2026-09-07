/*
 * TautologicalValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RelationalRecordQueryPlanProto.PTautologicalValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LeafValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;

import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

@API(API.Status.EXPERIMENTAL)
public final class TautologicalValue extends AbstractValue implements BooleanValue, LeafValue {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Tautological-Value");

    private static final TautologicalValue INSTANCE = new TautologicalValue();

    private TautologicalValue() {
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        Verify.verify(Iterables.isEmpty(explainSuppliers));
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword("TRUE"));
    }

    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nullable TypeRepository typeRepository,
                                                     Set<CorrelationIdentifier> localAliases) {
        return Optional.of(ConstantPredicate.TRUE);
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, EvaluationContext context) {
        return true;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(PlanHashMode mode) {
        return hashCodeWithoutChildren();
    }

    public static TautologicalValue getInstance() {
        return INSTANCE;
    }

    @Override
    public PValue toValueProto(final PlanSerializationContext serializationContext) {
        return PValue.newBuilder()
                .setAdditionalValues(PlanSerialization.protoObjectToAny(serializationContext,
                        toProto(serializationContext)))
                .build();
    }

    @Override
    public PTautologicalValue toProto(final PlanSerializationContext planSerializationContext) {
        return PTautologicalValue.newBuilder().build();
    }

    public static TautologicalValue fromProto(final PlanSerializationContext serializationContext,
                                              final PTautologicalValue tautologicalValueProto) {
        return getInstance();
    }

    @AutoService(PlanDeserializer.class)
    @SuppressWarnings("unused")
    public static class Deserializer implements PlanDeserializer<PTautologicalValue, TautologicalValue> {
        @Override
        public Class<PTautologicalValue> getProtoMessageClass() {
            return PTautologicalValue.class;
        }

        @Override
        public TautologicalValue fromProto(final PlanSerializationContext serializationContext,
                                           final PTautologicalValue tautologicalValueProto) {
            return TautologicalValue.fromProto(serializationContext, tautologicalValueProto);
        }
    }
}
