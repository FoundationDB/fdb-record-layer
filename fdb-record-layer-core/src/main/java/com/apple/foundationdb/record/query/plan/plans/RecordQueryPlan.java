/*
 * RecordQueryPlan.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * An executable query plan for producing records.
 *
 * A query plan is run against a record store to produce a stream of matching records.
 *
 * A query plan of any complexity will have child plans and execute by altering or combining the children's streams in some way.
 *
 * @see com.apple.foundationdb.record.query.RecordQuery
 * @see com.apple.foundationdb.record.query.plan.RecordQueryPlanner#plan(RecordQuery) 
 *
 */
@API(API.Status.STABLE)
public interface RecordQueryPlan extends QueryPlan<FDBQueriedRecord<Message>>, PlannerGraphRewritable {

    /**
     * Execute this query plan.
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @param <M> type used to represent stored records
     * @return a cursor of records that match the query criteria
     */
    @Nonnull
    default <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                  @Nonnull EvaluationContext context,
                                                                  @Nullable byte[] continuation,
                                                                  @Nonnull ExecuteProperties executeProperties) {
        return executePlan(store, context, continuation, executeProperties)
                .map(QueryResult::getQueriedRecord);
    }

    @Nonnull
    @Override
    default RecordCursor<FDBQueriedRecord<Message>> execute(@Nonnull FDBRecordStore store,
                                                            @Nonnull EvaluationContext context,
                                                            @Nullable byte[] continuation,
                                                            @Nonnull ExecuteProperties executeProperties) {
        return execute((FDBRecordStoreBase<Message>)store, context, continuation, executeProperties);
    }

    /**
     * Execute this query plan.
     * @param store record store from which to fetch records
     * @param <M> type used to represent stored records
     * @return a cursor of records that match the query criteria
     */
    @Nonnull
    default <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store) {
        return execute(store, EvaluationContext.EMPTY);
    }

    /**
     * Execute this query plan.
     * @param store record store to access
     * @param context evaluation context containing parameter bindings
     * @param <M> type used to represent stored records
     * @return a cursor of records that match the query criteria
     */
    @Nonnull
    default <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        return execute(store, context, null, ExecuteProperties.SERIAL_EXECUTE);
    }

    /**
     * Execute this plan, returning a {@link RecordCursor} to {@link QueryResult} result.
     * @param store record store from which to fetch records
     * @param context evaluation context containing parameter bindings
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @param <M> type used to represent stored records
     * @return a cursor of {@link QueryResult} that match the query criteria
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                              @Nonnull EvaluationContext context,
                                                              @Nullable byte[] continuation,
                                                              @Nonnull ExecuteProperties executeProperties);

    /**
     * Returns the (zero or more) {@code RecordQueryPlan} children of this plan.
     *
     * <p>
     * <b>Warning</b>: This part of the API is undergoing active development. At some point in the future,
     * the return type of this method will change to allow it to return a list of generic {@link QueryPlan}s.
     * At current, every {@code RecordQueryPlan} can only have other {@code RecordQueryPlan}s as children.
     * However, this is not guaranteed to be the case in the future. This method has been marked as
     * {@link API.Status#UNSTABLE} as of version 2.5.
     * </p>
     *
     * @return the child plans
     */
    @API(API.Status.UNSTABLE)
    @Nonnull
    List<RecordQueryPlan> getChildren();

    @Nonnull
    @Override
    default List<? extends QueryPlan<?>> getQueryPlanChildren() {
        return getChildren();
    }

    @Nonnull
    AvailableFields getAvailableFields();

    // we know the type of the group, even though the compiler doesn't, intentional use of reference equality
    @Nonnull
    @SuppressWarnings({"unchecked", "PMD.CompareObjectsWithEquals"})
    default RecordQueryPlan accept(@Nonnull RecordQueryPlannerSubstitutionVisitor visitor) {
        // Using a quantifier here is a bit of a hack since quantifiers only make conceptual sense in the data model
        // of the experimental planner. However, they provide access to the underlying groups, which allow us to
        // substitute children without adding additional mutable access to the children of a query plan.
        // This makes some amount of conceptual sense, too, since we're performing the same kind of substitution
        // that a PlannerRule would provide.

        for (Quantifier childQuantifier : getQuantifiers()) {
            if (!(childQuantifier instanceof Quantifier.Physical)) {
                throw new RecordCoreException("quantifiers of RecordQueryPlans must be physical");
            }
            // Group expression is the only type of reference at this point.
            GroupExpressionRef<RecordQueryPlan> childGroup = ((GroupExpressionRef)((Quantifier.Physical)childQuantifier).getRangesOver());
            RecordQueryPlan child = childGroup.get(); // Group is generated by the RecordQueryPlanner so must have a single member

            RecordQueryPlan modifiedChild = child.accept(visitor);
            if (child != modifiedChild) { // intentional use of reference equality, since equals() might not be conservative enough for plans
                childGroup.replace(modifiedChild);
            }
        }

        return visitor.postVisit(this);
    }

    /**
     * Return a hash code for this plan which is defined based on the structural layout of a plan. This differs from
     * the semantic hash code defined in {@link RelationalExpression}. For instance this method would not necessarily return
     * the same hash code for a union {@code UNION(p1, p2)} of two sub-plans {@code p1} and {@code p2} and it's reversed
     * {@code UNION(p2, p1)}. In contrast to that the semantic hash of these two plans is the same.
     * @return a hash code for this objects that is defined on the structural layout of the plan
     */
    @API(API.Status.EXPERIMENTAL)
    default int structuralHashCode() {
        return Objects.hash(getQuantifiers(), hashCodeWithoutChildren());
    }

    /**
     * Overloaded method to determine structural equality between two different plans using an empty {@link AliasMap}.
     * @param other object to compare this object with
     * @return {@code true} if {@code this} is structurally equal to {@code other}, {@code false} otherwise
     */
    @API(API.Status.EXPERIMENTAL)
    default boolean structuralEquals(@Nullable final Object other) {
        return structuralEquals(other, AliasMap.emptyMap());
    }

    /**
     * Determine if two plans are structurally equal. This differs from the semantic equality defined in
     * {@link RelationalExpression}. For instance this method would return false
     * for two given plans {@code UNION(p1, p2)} and {@code UNION(p2, p1)} of two different sub-plans {@code p1} and
     * {@code p2}. In contrast to that these plans are considered semantically equal.
     * @param other object to compare this object with
     * @param equivalenceMap alias map to indicate aliases that should be considered as equal when {@code other} is
     *        compared to {@code this}. For instance {@code q1.x = 1} is only structurally equal with {@code q2.x = 1}
     *        if there is a mapping {@code q1 -> q2} in the alias map passed in
     * @return {@code true} if {@code this} is structurally equal to {@code other}, {@code false} otherwise
     */
    @API(API.Status.EXPERIMENTAL)
    default boolean structuralEquals(@Nullable final Object other,
                                     @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final RelationalExpression otherExpression = (RelationalExpression)other;

        // We know this and otherExpression are of the same class. canCorrelate() needs to match as well.
        Verify.verify(canCorrelate() == otherExpression.canCorrelate());

        final List<Quantifier.Physical> quantifiers =
                Quantifiers.narrow(Quantifier.Physical.class,
                        getQuantifiers());
        final List<Quantifier.Physical> otherQuantifiers =
                Quantifiers.narrow(Quantifier.Physical.class,
                        otherExpression.getQuantifiers());

        if (quantifiers.size() != otherQuantifiers.size()) {
            return false;
        }

        final Iterable<AliasMap> boundCorrelatedReferencesIterable =
                enumerateUnboundCorrelatedTo(equivalenceMap, otherExpression);

        for (final AliasMap boundCorrelatedReferencesMap : boundCorrelatedReferencesIterable) {
            final AliasMap.Builder boundCorrelatedToBuilder = boundCorrelatedReferencesMap.derived();

            AliasMap boundCorrelatedToMap = AliasMap.emptyMap();

            int i;
            for (i = 0; i < quantifiers.size(); i++) {
                boundCorrelatedToMap = boundCorrelatedToBuilder.build();

                final Quantifier.Physical quantifier = quantifiers.get(i);
                final Quantifier.Physical otherQuantifier = otherQuantifiers.get(i);

                if (quantifier.structuralHashCode() != otherQuantifier.structuralHashCode()) {
                    break;
                }

                if (!quantifier.structuralEquals(otherQuantifier)) {
                    break;
                }

                if (canCorrelate()) {
                    boundCorrelatedToBuilder.put(quantifier.getAlias(), otherQuantifier.getAlias());
                }
            }

            if (i == quantifiers.size() && (equalsWithoutChildren(otherExpression, boundCorrelatedToMap))) {
                return true;
            }
        }

        return false;
    }

    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    @SuppressWarnings("unchecked")
    static <R extends RecordQueryPlan> ExpressionRef<R> narrowReference(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
        Verify.verify(reference.getMembers().stream().allMatch(member -> member instanceof RecordQueryPlan));
        return (GroupExpressionRef<R>)reference;
    }
}
