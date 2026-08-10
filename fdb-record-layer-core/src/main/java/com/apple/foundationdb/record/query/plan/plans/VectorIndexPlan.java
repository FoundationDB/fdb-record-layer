/*
 * VectorIndexPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PVectorIndexPlan;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexEngineKind;
import com.apple.foundationdb.record.query.plan.IndexTraversalKind;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A query plan that scans a {@link com.apple.foundationdb.record.metadata.IndexTypes#VECTOR vector} index. It is an
 * ordinary {@link RecordQueryIndexPlan} in every respect but one: it also carries the
 * {@link VectorIndexEngineKind engine} backing the index it scans, because a vector scan does not say which structure
 * it walks. The scan parameters of every vector scan are alike, while the structure underneath is an
 * {@link com.apple.foundationdb.record.metadata.IndexOptions#VECTOR_ENGINE index option}, so the engine has to be
 * carried on the plan for {@link #getIndexTraversalKind()} to be able to answer.
 * <p>
 * Nothing constructs this plan yet, the planner still produces a plain {@link RecordQueryIndexPlan} for a vector index
 * scan, and the only way to get one of these is to deserialize it. The class exists ahead of that so the serialization
 * format is in place before plans in this shape are written.
 *
 * @see IndexTraversalKind
 */
@API(API.Status.INTERNAL)
@SpotBugsSuppressWarnings("EQ_DOESNT_OVERRIDE_EQUALS") // equality reaches the engine through equalsWithoutChildren()
public class VectorIndexPlan extends RecordQueryIndexPlan {

    @Nonnull
    protected static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Vector-Index-Plan");

    @Nonnull
    private final VectorIndexEngineKind engine;

    protected VectorIndexPlan(@Nonnull final PlanSerializationContext serializationContext,
                              @Nonnull final PVectorIndexPlan vectorIndexPlanProto) {
        super(serializationContext, Objects.requireNonNull(vectorIndexPlanProto.getSuper()));
        //
        // Insist on the engine rather than letting the proto default stand in for it: defaulting to one engine would
        // quietly report the wrong traversal for a plan written by anything that did not set it.
        //
        Verify.verify(vectorIndexPlanProto.hasEngine());
        this.engine = VectorIndexEngineKind.fromProto(vectorIndexPlanProto.getEngine());
    }

    // TODO Once the planner starts creating this plan, the methods that copy an index plan — strictlySorted(),
    //      minimize() and withIndexScanParameters() — need overriding here, as the ones inherited from
    //      RecordQueryIndexPlan return a plain RecordQueryIndexPlan and would drop the engine.

    /**
     * Returns the engine backing the index this plan scans.
     *
     * @return the engine backing the index this plan scans
     */
    @Nonnull
    public VectorIndexEngineKind getEngine() {
        return engine;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Answered from the engine rather than from the scan parameters, which are the same for every vector scan however
     * the index is built.
     *
     * @return the way this plan traverses the index
     */
    @Nonnull
    @Override
    public IndexTraversalKind getIndexTraversalKind() {
        return switch (engine) {
            case HNSW -> IndexTraversalKind.HNSW;
            case GUARDIANN -> IndexTraversalKind.GUARDIANN;
            default -> throw new RecordCoreException("unknown vector index engine mapping. did you forget to add it?");
        };
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (!super.equalsWithoutChildren(otherExpression, equivalencesMap)) {
            return false;
        }
        return engine == ((VectorIndexPlan)otherExpression).engine;
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(super.computeHashCodeWithoutChildren(), engine);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return switch (mode.getKind()) {
            case LEGACY, FOR_CONTINUATION ->
                    PlanHashable.objectsPlanHash(mode, BASE_HASH, indexName, scanParameters, reverse, strictlySorted, engine);
            default -> throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        };
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return toVectorIndexPlanProto(serializationContext);
    }

    @Nonnull
    public PVectorIndexPlan toVectorIndexPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PVectorIndexPlan.newBuilder()
                .setSuper(toRecordQueryIndexPlanProto(serializationContext))
                .setEngine(engine.toProto())
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setVectorIndexPlan(toVectorIndexPlanProto(serializationContext)).build();
    }

    @Nonnull
    public static VectorIndexPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PVectorIndexPlan vectorIndexPlanProto) {
        return new VectorIndexPlan(serializationContext, vectorIndexPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PVectorIndexPlan, VectorIndexPlan> {
        @Nonnull
        @Override
        public Class<PVectorIndexPlan> getProtoMessageClass() {
            return PVectorIndexPlan.class;
        }

        @Nonnull
        @Override
        public VectorIndexPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PVectorIndexPlan vectorIndexPlanProto) {
            return VectorIndexPlan.fromProto(serializationContext, vectorIndexPlanProto);
        }
    }
}
