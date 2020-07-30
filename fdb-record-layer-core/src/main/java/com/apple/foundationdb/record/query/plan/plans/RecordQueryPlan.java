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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * An executable query plan for producing records.
 *
 * A query plan is run against a record store to produce a stream of matching records.
 *
 * A query plan of any complexity will have child plans and execute by altering or combining the children's streams in some way.
 *
 * @see com.apple.foundationdb.record.query.RecordQuery
 * @see com.apple.foundationdb.record.query.plan.RecordQueryPlanner#plan
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
    <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                  @Nonnull EvaluationContext context,
                                                                  @Nullable byte[] continuation,
                                                                  @Nonnull ExecuteProperties executeProperties);

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
    default RecordQueryPlan accept(@Nonnull RecordQueryPlannerSubstitutionVisitor visitor) {
        return accept(visitor, Collections.emptySet());
    }

    // we know the type of the group, even though the compiler doesn't, intentional use of reference equality
    @Nonnull
    @SuppressWarnings({"unchecked", "PMD.CompareObjectsWithEquals"})
    default RecordQueryPlan accept(@Nonnull RecordQueryPlannerSubstitutionVisitor visitor, @Nonnull Set<KeyExpression> parentRequiredFields) {
        // Using a quantifier here is a bit of a hack since quantifiers only make conceptual sense in the data model
        // of the experimental planner. However, they provide access to the underlying groups, which allow us to
        // substitute children without adding additional mutable access to the children of a query plan.
        // This makes some amount of conceptual sense, too, since we're performing the same kind of substitution
        // that a PlannerRule would provide.
        Set<KeyExpression> requiredFields = visitor.preVisitForRequiredFields(this, parentRequiredFields);

        for (Quantifier childQuantifier : getQuantifiers()) {
            if (!(childQuantifier instanceof Quantifier.Physical)) {
                throw new RecordCoreException("quantifiers of RecordQueryPlans must be physical");
            }
            // Group expression is the only type of reference at this point.
            GroupExpressionRef<RecordQueryPlan> childGroup = ((GroupExpressionRef)((Quantifier.Physical)childQuantifier).getRangesOver());
            RecordQueryPlan child = childGroup.get(); // Group is generated by the RecordQueryPlanner so must have a single member

            RecordQueryPlan modifiedChild = child.accept(visitor, requiredFields);
            if (child != modifiedChild) { // intentional use of reference equality, since equals() might not be conservative enough for plans
                childGroup.clear();
                childGroup.insert(modifiedChild);
            }
        }
        return visitor.postVisit(this, requiredFields);
    }
}
