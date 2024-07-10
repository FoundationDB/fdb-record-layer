/*
 * RecordQueryInJoinPlan.java
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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryInJoinPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that executes a child plan once for each of the elements of some {@code IN} list.
 */
@API(API.Status.INTERNAL)
public abstract class RecordQueryInJoinPlan implements RecordQueryPlanWithChild {
    @Nonnull
    protected final Quantifier.Physical inner;
    @Nonnull
    protected final InSource inSource;

    /**
     * The use of this field is to distinguish old-planner use of this plan object versus cascades planner use.
     * If created by the heuristic recursive descent planner (the old planner), it is expected that the binding it
     * creates for the evaluation of the inner is participating in the plan hash. Unfortunately, this cannot be
     * done when using the Cascades planner as the planner uses identifiers that are not stable across plannings.
     * <br>
     * The binding internal has to be set to either {@link Bindings.Internal#IN} if the object is created by the old
     * planner of to {@link Bindings.Internal#CORRELATION} if the object is created by the new planner.
     */
    @Nonnull
    protected final Bindings.Internal internal;

    protected RecordQueryInJoinPlan(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PRecordQueryInJoinPlan inJoinPlanProto) {
        this(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(inJoinPlanProto.getPhysicalQuantifier())),
                InSource.fromInSourceProto(serializationContext, Objects.requireNonNull(inJoinPlanProto.getInSource())),
                Bindings.Internal.fromProto(serializationContext, Objects.requireNonNull(inJoinPlanProto.getInternal())));
    }

    protected RecordQueryInJoinPlan(@Nonnull final Quantifier.Physical inner,
                                    @Nonnull final InSource inSource,
                                    @Nonnull final Bindings.Internal internal) {
        Verify.verify(internal == Bindings.Internal.IN || internal == Bindings.Internal.CORRELATION);
        this.inner = inner;
        this.inSource = inSource;
        this.internal = internal;
    }

    @Nonnull
    public Quantifier.Physical getInner() {
        return inner;
    }

    @Nonnull
    public InSource getInSource() {
        return inSource;
    }

    @Nonnull
    public CorrelationIdentifier getInAlias() {
        return CorrelationIdentifier.of(internal.identifier(inSource.getBindingName()));
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return RecordCursor.flatMapPipelined(
                outerContinuation -> RecordCursor.fromList(store.getExecutor(), getValues(context), outerContinuation),
                        (outerValue, innerContinuation) -> {
                            final Object bindingValue = internal == Bindings.Internal.IN ? outerValue : QueryResult.ofComputed(outerValue);
                            return getInnerPlan().executePlan(store, context.withBinding(inSource.getBindingName(), bindingValue),
                                    innerContinuation, executeProperties.clearSkipAndLimit());
                        },
                        outerObject -> {
                            if (outerObject instanceof DynamicMessage) {
                                return ((DynamicMessage) outerObject).toByteArray();
                            } else {
                                return Tuple.from(ScanComparisons.toTupleItem(outerObject)).pack();
                            }
                        },
                        continuation,
                        store.getPipelineSize(PipelineOperation.IN_JOIN))
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public boolean isReverse() {
        if (inSource.isSorted()) {
            return inSource.isReverse();
        } else {
            throw new RecordCoreException("RecordQueryInJoinPlan does not have well defined reverse-ness");
        }
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        final var resultType = inSource.getResultType();

        if (!resultType.isAny()) {
            final var resultTypesBuilder = ImmutableSet.<Type>builder();
            resultTypesBuilder.addAll(RecordQueryPlanWithChild.super.getDynamicTypes());
            resultTypesBuilder.add(resultType);
            return resultTypesBuilder.build();
        } else {
            return RecordQueryPlanWithChild.super.getDynamicTypes();
        }
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();

        final var inAlias = getInAlias();
        inner.getCorrelatedTo()
                .stream()
                // filter out the correlations that are satisfied by this plan
                .filter(alias -> !alias.equals(inAlias))
                .forEach(builder::add);

        return builder.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public boolean canCorrelate() {
        return true;
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
        final RecordQueryInJoinPlan other = (RecordQueryInJoinPlan)otherExpression;
        return inSource.equals(other.inSource);
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
        return inSource.hashCode();
    }

    /**
     * Base implementation of {@link #planHash(PlanHashMode)}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash(PlanHashMode)} so
     * that they are guided to add their own class modifier (See {@link ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param mode the plan hash mode to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    @SuppressWarnings("fallthrough")
    protected int basePlanHash(@Nonnull final PlanHashMode mode, ObjectPlanHash baseHash, Object... hashables) {
        switch (mode.getKind()) {
            case LEGACY:
                if (internal == Bindings.Internal.IN) {
                    return getInnerPlan().planHash(mode) +
                           inSource.getBindingName().hashCode() +
                           (inSource.isSorted() ? 1 : 0) +
                           (inSource.isReverse() ? 1 : 0);
                }
                // fall through
            case FOR_CONTINUATION:
                if (internal == Bindings.Internal.IN) {
                    return PlanHashable.objectsPlanHash(mode,
                            baseHash,
                            getInnerPlan(),
                            inSource.getBindingName(),
                            inSource.isSorted(),
                            inSource.isReverse(),
                            hashables);
                }
                return PlanHashable.objectsPlanHash(mode, baseHash, getInnerPlan(), inSource, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    protected List<Object> getValues(EvaluationContext context) {
        return inSource.getValues(context);
    }

    @Override
    public int getComplexity() {
        return 1 + getInnerPlan().getComplexity();
    }

    @Nonnull
    public PRecordQueryInJoinPlan toRecordQueryInJoinPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryInJoinPlan.newBuilder()
                .setPhysicalQuantifier(inner.toProto(serializationContext))
                .setInSource(inSource.toInSourceProto(serializationContext))
                .setInternal(internal.toProto(serializationContext))
                .build();
    }
}
