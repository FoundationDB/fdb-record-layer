/*
 * RecordQueryExplodePlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.EmptyCursor;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.cursors.MapCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.Formatter;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that reconstructs records from the entries in a covering index.
 */
@API(API.Status.INTERNAL)
public class RecordQueryExplodePlan implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Map-Plan");

    @Nonnull
    private final Value resultValue;

    public RecordQueryExplodePlan(@Nonnull Value resultValue) {
        this.resultValue = resultValue;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final Type.Array valueResultType = (Type.Array)resultValue.getResultType();

        List<Object> arrayAsList = (List<Object>)resultValue.eval(store, context, null, null);

        if (arrayAsList == null || arrayAsList.isEmpty()) {
            return new EmptyCursor<>(store.getExecutor());
        }

        final ListCursor<Object> listCursor = new ListCursor<>(store.getExecutor(), arrayAsList, continuation);

        //
        // Stick the elements in a QueryResult each -- but only when streamed in order to not have to create all
        // the wrappers up-front.
        //
        return new MapCursor<>(listCursor,
                element -> {
                    final Object normalizedElement;
                    if (valueResultType.needsNestedProto()) {
                        final Message message = (Message)element;
                        final Descriptors.Descriptor elementDescriptor = message.getDescriptorForType();
                        final Descriptors.FieldDescriptor valueFieldDescriptor = elementDescriptor.findFieldByNumber(1);
                        if (message.getRepeatedFieldCount(valueFieldDescriptor) > 0) {
                            normalizedElement = message.getField(valueFieldDescriptor);
                        } else {
                            normalizedElement = null;
                        }
                    } else {
                        normalizedElement = element;
                    }

                    if (normalizedElement instanceof QueryResultElement) {
                        return QueryResult.of((QueryResultElement)normalizedElement);
                    } else {
                        return QueryResult.of(QueryResultElement.Wrapped.of(normalizedElement));
                    }
                });
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
        return ImmutableSet.of();
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return resultValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RecordQueryExplodePlan rebase(@Nonnull final AliasMap translationMap) {
        return new RecordQueryExplodePlan(resultValue.rebase(translationMap));
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public boolean isStrictlySorted() {
        return false;
    }

    @Override
    public RecordQueryExplodePlan strictlySorted() {
        return this;
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValues() {
        return ImmutableList.of(resultValue);
    }

    @Nonnull
    @Override
    public Type.Stream getResultType() {
        Verify.verify(resultValue.getResultType().getTypeCode() == Type.TypeCode.ARRAY,
                "cannot perform explode on type " + resultValue.getResultType());

        final Type innerType = ((Type.Array)resultValue.getResultType()).getElementType();
        return new Type.Stream(new Type.Tuple(ImmutableList.of(Objects.requireNonNull(innerType))));
    }

    @Nonnull
    @Override
    public String toString() {
        return "explode(" + resultValue + ")";
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "explode(" + resultValue.explain(formatter) + ")";
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return semanticEqualsForResults(otherExpression, aliasMap);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getResultValues());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getResultValues());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("EXPLODE {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(resultValue))),
                childGraphs);
    }
}
