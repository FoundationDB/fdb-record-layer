/*
 * RecordQueryExplodePlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryExplodePlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithoutChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * An {@code EXPLODE} query plan.
 *
 * <p>{@code EXPLODE} is the plan node that implements <em>array unnesting</em>, also known as the “explode”
 * operation. It is a leaf plan node. The array data comes from its {@link #collectionValue} member, which is a
 * correlated {@link Value} referencing the field of an outer quantifier. The plan node first evaluates the collection
 * value to Java type {@code List<?>}, and then produces one {@link QueryResult} datum per array element.
 *
 * <p>In the {@code WITH ORDINALITY} variant, {@code EXPLODE} also generates ordinals of the array elements. In this
 * case the plan produces a {@link DynamicMessage} struct with two anonymous fields (the element and the ordinal)
 * instead of the bare element. The ordinals are 1-based per the SQL standard (Foundation, Section 4.10.2).
 *
 * @see RecordQueryFlatMapPlan
 */
@API(API.Status.INTERNAL)
public class RecordQueryExplodePlan extends AbstractRelationalExpressionWithoutChildren implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Explode-Plan");

    /**
     * The collection value. Must evaluate to an array type.
     */
    @Nonnull
    private final Value collectionValue;

    /**
     * Whether ordinals should be produced alongside the array elements.
     */
    private final boolean withOrdinality;

    /**
     * The element type of the collection value.
     */
    @Nonnull
    private final Type elementType;

    /**
     * The type of the explode result.
     */
    @Nonnull
    private final Type explodeResultType;

    public RecordQueryExplodePlan(@Nonnull Value collectionValue, boolean withOrdinality) {
        this.collectionValue = collectionValue;
        this.withOrdinality = withOrdinality;
        Verify.verify(collectionValue.getResultType().isArray());
        this.elementType = Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType());
        this.explodeResultType = ExplodeExpression.explodeResultType(elementType, withOrdinality);
    }

    public RecordQueryExplodePlan(@Nonnull Value collectionValue) {
        this(collectionValue, false);
    }

    @Nonnull
    public Value getCollectionValue() {
        return collectionValue;
    }

    public boolean isWithOrdinality() {
        return withOrdinality;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final Object result = collectionValue.eval(store, context);
        final List<?> list = (result == null) ? List.of() : (List<?>)result;

        // Without ordinality, produce the bare elements.
        if (!withOrdinality) {
            return RecordCursor.fromList(list, continuation)
                    .map(QueryResult::ofComputed)
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        }

        // In the WITH ORDINALITY case, produce a struct (element, ordinal) per list element, with 1-based ordinals.
        final Type elementType = getElementType();
        final var resultType = (Type.Record) getExplodeResultType();
        final TypeRepository typeRepository = context.getTypeRepository();
        final Descriptors.Descriptor descriptor = Objects.requireNonNull(typeRepository.getMessageDescriptor(resultType));
        final Descriptors.FieldDescriptor elementField = descriptor.getFields().get(0);
        final Descriptors.FieldDescriptor ordinalField = descriptor.getFields().get(1);
        final ImmutableList<Message> indexedList =
                IntStream.rangeClosed(1, list.size())
                .mapToObj(i -> {
                    final DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
                    final Object element = Verify.verifyNotNull(list.get(i - 1), "array elements must be non-null");
                    builder.setField(elementField,
                            RecordConstructorValue.deepCopyIfNeeded(typeRepository, elementType, element));
                    builder.setField(ordinalField, i);
                    return (Message)builder.build();
                })
                .collect(ImmutableList.toImmutableList());
        return RecordCursor.fromList(indexedList, continuation)
                .map(QueryResult::ofComputed)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return collectionValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryExplodePlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                        final boolean shouldSimplifyValues,
                                                        @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        final Value translatedCollectionValue =
                collectionValue.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedCollectionValue != collectionValue) {
            return new RecordQueryExplodePlan(translatedCollectionValue, withOrdinality);
        }
        return this;
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public RecordQueryExplodePlan strictlySorted(@Nonnull final FinalMemoizer memoizer) {
        return this;
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
        return AvailableFields.NO_FIELDS;
    }

    /**
     * Returns the element type of the collection value.
     */
    @Nonnull
    public Type getElementType() {
        return elementType;
    }

    /**
     * Returns the type of the explode result.
     */
    @Nonnull
    public Type getExplodeResultType() {
        return explodeResultType;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(getExplodeResultType());
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        return ImmutableSet.<Type>builder()
                .addAll(collectionValue.getDynamicTypes())
                .addAll(getResultValue().getDynamicTypes())
                .build();
    }

    @Nonnull
    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherExplodePlan = (RecordQueryExplodePlan)otherExpression;
        return collectionValue.semanticEquals(otherExplodePlan.getCollectionValue(), equivalencesMap) &&
                isWithOrdinality() == otherExplodePlan.isWithOrdinality() &&
                semanticEqualsForResults(otherExpression, equivalencesMap);
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
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
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY, FOR_CONTINUATION -> {
                // Note: This is written in a way that preserves pre-existing hashes for `withOrdinality=false`.
                final Value result = getResultValue();
                return withOrdinality ? PlanHashable.objectsPlanHash(mode, BASE_HASH, result, true)
                                      : PlanHashable.objectsPlanHash(mode, BASE_HASH, result);
            }
            default -> throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final String label = withOrdinality ? "EXPLODE {{expr}} WITH ORDINALITY"
                                            : "EXPLODE {{expr}}";
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of(label),
                        ImmutableMap.of("expr", Attribute.gml(collectionValue.toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryExplodePlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PRecordQueryExplodePlan.newBuilder();
        builder.setCollectionValue(collectionValue.toValueProto(serializationContext));
        if (withOrdinality) {
            builder.setWithOrdinality(true);
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setExplodePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryExplodePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordQueryExplodePlan proto) {
        return new RecordQueryExplodePlan(
                Value.fromValueProto(serializationContext, Objects.requireNonNull(proto.getCollectionValue())),
                proto.getWithOrdinality());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryExplodePlan, RecordQueryExplodePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryExplodePlan> getProtoMessageClass() {
            return PRecordQueryExplodePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryExplodePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQueryExplodePlan proto) {
            return RecordQueryExplodePlan.fromProto(serializationContext, proto);
        }
    }
}
