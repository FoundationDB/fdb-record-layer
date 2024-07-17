/*
 * QueryRecordFunction.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexFunctionHelper;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Class that provides context for asserting about a specially calculated value.
 * @param <T> the result type of the function
 */
@API(API.Status.MAINTAINED)
public class QueryRecordFunction<T> implements PlanHashable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Query-Record-Function");

    @Nonnull
    private final RecordFunction<T> function;
    @Nullable
    private final QueryComponent additionalCondition;
    @Nullable
    private RecordFunction<T> evalFunction;

    public QueryRecordFunction(@Nonnull RecordFunction<T> function, @Nullable final QueryComponent additionalCondition) {
        this.function = function;
        this.additionalCondition = additionalCondition;
    }

    public QueryRecordFunction(@Nonnull RecordFunction<T> function) {
        this(function, null);
    }

    @Nonnull
    public RecordFunction<T> getFunction() {
        return function;
    }

    @Nonnull
    public QueryRecordFunction<T> withFunction(@Nonnull RecordFunction<T> function) {
        return new QueryRecordFunction<>(function, additionalCondition);
    }

    @Nullable
    public QueryComponent getAdditionalCondition() {
        return additionalCondition;
    }

    @Nonnull
    public QueryRecordFunction<T> withAdditionalCondition(@Nonnull QueryComponent additionalCondition) {
        return new QueryRecordFunction<>(function, and(additionalCondition));
    }

    @Nonnull
    private  QueryComponent and(@Nonnull QueryComponent condition) {
        if (additionalCondition == null) {
            return condition;
        } else if (additionalCondition instanceof AndComponent) {
            List<QueryComponent> children = new ArrayList<>(((AndComponent)additionalCondition).getChildren());
            children.add(condition);
            return Query.and(children);
        } else {
            return Query.and(additionalCondition, condition);
        }
    }

    // TODO: Perhaps these Object comparands should really be T. Right now everything is <Long>.

    /**
     * Checks if the calculated value has a value equal to the given comparand.
     * @param comparand the object to compare with the value in the calculated value
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent equalsValue(@Nonnull Object comparand) {
        return withComparison(Comparisons.Type.EQUALS, comparand);
    }

    /**
     * Checks if the calculated value has a value not equal to the given comparand.
     * @param comparand the object to compare with the value in the calculated value
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent notEquals(@Nonnull Object comparand) {
        return withComparison(Comparisons.Type.NOT_EQUALS, comparand);
    }

    /**
     * Checks if the calculated value has a value greater than the given comparand.
     * @param comparand the object to compare with the value in the calculated value
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThan(@Nonnull Object comparand) {
        return withComparison(Comparisons.Type.GREATER_THAN, comparand);
    }

    /**
     * Checks if the calculated value has a value greater than or equal to the given comparand.
     * @param comparand the object to compare with the value in the calculated value
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent greaterThanOrEquals(@Nonnull Object comparand) {
        return withComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, comparand);
    }

    /**
     * Checks if the calculated value has a value less than the given comparand.
     * @param comparand the object to compare with the value in the calculated value
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThan(@Nonnull Object comparand) {
        return withComparison(Comparisons.Type.LESS_THAN, comparand);
    }

    /**
     * Checks if the calculated value has a value less than or equal to the given comparand.
     * @param comparand the object to compare with the value in the calculated value
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent lessThanOrEquals(@Nonnull Object comparand) {
        return withComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, comparand);
    }


    /**
     * Checks if the result for this function is in the given list.
     * @param comparand a list of elements
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent in(@Nonnull List<?> comparand) {
        return withComparison(new Comparisons.ListComparison(Comparisons.Type.IN, comparand));
    }

    /**
     * Checks if the result for this function is in the list that is bound to the given param.
     * @param param a param that will be bound to a list in the execution context
     * @return a new component for doing the actual evaluation
     */
    @Nonnull
    public QueryComponent in(@Nonnull String param) {
        return withParameterComparison(Comparisons.Type.IN, param);
    }

    @Nonnull
    public QueryComponent withComparison(@Nonnull Comparisons.Comparison comparison) {
        return and(new QueryRecordFunctionWithComparison(function, comparison));
    }

    @Nonnull
    public QueryComponent withComparison(@Nonnull Comparisons.Type type, @Nonnull Object comparand) {
        return withComparison(new Comparisons.SimpleComparison(type, comparand));
    }

    @Nonnull
    public QueryComponent withParameterComparison(@Nonnull Comparisons.Type type, String parameter) {
        return withComparison(new Comparisons.ParameterComparison(type, parameter));
    }

    public <M extends Message> CompletableFuture<T> eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable FDBStoredRecord<M> record) {
        if (record == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (additionalCondition == null) {
            return store.evaluateRecordFunction(context, function, record);
        }
        // Cache the bound version of the record function.
        if (evalFunction == null) {
            evalFunction = IndexFunctionHelper.recordFunctionWithSubrecordCondition(store.getUntypedRecordStore(), (IndexRecordFunction<T>) function, record, additionalCondition);
        }
        return store.evaluateRecordFunction(context, evalFunction, record);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryRecordFunction<?> that = (QueryRecordFunction)obj;

        return this.function.equals(that.function) && Objects.equals(this.additionalCondition, that.additionalCondition);
    }

    @Override
    public int hashCode() {
        return function.hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return function.planHash(mode);
            case FOR_CONTINUATION:
                if (additionalCondition == null) {
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, function);
                } else {
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, function, additionalCondition);
                }
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        if (additionalCondition == null) {
            return function.toString();
        } else {
            return function + " | " + additionalCondition;
        }
    }

}
