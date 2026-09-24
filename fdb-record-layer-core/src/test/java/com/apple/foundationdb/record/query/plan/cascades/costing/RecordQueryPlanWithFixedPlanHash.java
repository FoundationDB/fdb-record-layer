/*
 * RecordQueryPlanWithFixedPlanHash.java
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An implementation of {@link RecordQueryPlan} that is designed to allow us to pick a fixed plan hash
 * value for testing purposes.
 */
class RecordQueryPlanWithFixedPlanHash implements RecordQueryPlan {
    @Nonnull
    private final String name;
    private final int planHash;

    public RecordQueryPlanWithFixedPlanHash(@Nonnull String name, int planHash) {
        this.name = name;
        this.planHash = planHash;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final byte[] continuation, @Nonnull final ExecuteProperties executeProperties) {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return List.of();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Override
    @Nonnull
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull final String indexName) {
        return false;
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return Set.of();
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {

    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return planHash;
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return LiteralValue.ofScalar(42L);
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return List.of();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Set.of();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (!getClass().equals(other.getClass())) {
            return false;
        }
        final RecordQueryPlanWithFixedPlanHash otherWithFixedPlanHash = (RecordQueryPlanWithFixedPlanHash) other;
        return Objects.equals(name, otherWithFixedPlanHash.getName()) && planHash == otherWithFixedPlanHash.planHash;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return planHash;
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, final boolean shouldSimplifyValues, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return this;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return Set.of();
    }

    @Override
    public String toString() {
        return "FIXEDHASH(" + name + ": " + planHash + ")";
    }
}
