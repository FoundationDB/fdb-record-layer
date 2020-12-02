/*
 * QueryComponent.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpandedPredicates;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Base component interface for checking whether a given record matches a query.
 * Although you can provide your own implementations, let it be said that the planner does a lot of instanceof
 * checks to determine which indexes to use, and thus will end up falling back to getting all records and then
 * evaluating the component on the returned records.
 * {@link #validate(Descriptors.Descriptor)} must be called before calling {@link #eval(FDBRecordStoreBase, EvaluationContext, FDBRecord)}, or bad
 * things may happen.
 */
@API(API.Status.STABLE)
public interface QueryComponent extends PlanHashable {
    /**
     * Return whether or not the given record matches this component.
     * This may return true or false, but it may also return null. Generally a component should return null if some
     * data in the record is missing. e.g. if record does not have a value for field f, and this component is
     * checking whether the value f is greater than 3, it will probably want to return null. Other types of
     * component will naturally handle a value of null, such as isNull.
     *
     * Excepting for some outer
     * wrapping components (e.g. isNull), a null return value should not be included in results.
     *
     *
     * Implementations should override {@link #evalMessage} instead of this one, even if they do not deal with
     * Protobuf messages, so that they interact properly with expressions that do.
     * @param <M> the type of records
     * @param store the record store from which the record came
     * @param context context against which evaluation takes place
     * @param record a record of the appropriate record type for this component
     * @return true/false/null, true if the given record should be included in results, false if it should not, and
     * null if this component cannot determine whether it should be included or not
     */
    @Nullable
    default <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                             @Nullable FDBRecord<M> record) {
        return evalMessage(store, context, record, record == null ? null : record.getRecord());
    }

    /**
     * Return whether or not the given record matches this component.
     *
     * The message might be the Protobuf form of a record or a piece of that record.
     * If the key expression is meaningful against a subrecord, it should evaluate against the message.
     * Otherwise, it should evaluate against the record and ignore what part of that record is being considered.
     *
     * There should not be any reason to call this method outside of the implementation of another {@code evalMessage}.
     * Under ordinary circumstances, if {@code record} is {@code null}, then {@code message} will be {@code null}.
     * Otherwise, {@code message} will be {@code record.getRecord()} or some submessage of that, possibly {@code null} if
     * the corresponding field is missing.
     * @see #eval
     * @param <M> the type of record
     * @param store the record store from which the record came
     * @param context context for bound expressions
     * @param record the record
     * @param message the Protobuf message to evaluate against
     * @return true/false/null, true if the given record should be included in results, false if it should not, and
     * null if this component cannot determine whether it should be included or not
     */
    @Nullable
    <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                            @Nullable FDBRecord<M> record, @Nullable Message message);

    /**
     * Asynchronous version of {@code eval}.
     * @param <M> the type of records
     * @param store the record store from which the record came
     * @param context context against which evaluation takes place
     * @param record a record of the appropriate record type for this component
     * @return a future that completes with whether the record should be included in the query result
     * @see #eval
     */
    @Nonnull
    default <M extends Message> CompletableFuture<Boolean> evalAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                     @Nullable FDBRecord<M> record) {
        return evalMessageAsync(store, context, record, record == null ? null : record.getRecord());
    }

    /**
     * Asynchronous version of {@code evalMessage}.
     * @see #eval
     * @param <M> the type of record
     * @param store the record store from which the record came
     * @param context context for bound expressions
     * @param record the record
     * @param message the Protobuf message to evaluate against
     * @return a future that completes with whether the record should be included in the query result
     */
    @Nonnull
    default <M extends Message> CompletableFuture<Boolean> evalMessageAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                            @Nullable FDBRecord<M> record, @Nullable Message message) {
        return CompletableFuture.completedFuture(evalMessage(store, context, record, message));
    }

    /**
     * Get whether this component is asynchronous.
     * @return {@code true} if this component is better executed asynchronously
     */
    default boolean isAsync() {
        return false;
    }

    /**
     * Validate that the given descriptor is consistent with this component. e.g. it has all the fields defined that
     * this component wants to inspect.
     * @param descriptor a record type descriptor, or a submessage descriptor
     * @throws Query.InvalidExpressionException if the descriptor is not consistent with this component
     */
    void validate(@Nonnull Descriptors.Descriptor descriptor);

    @API(API.Status.EXPERIMENTAL)
    default ExpandedPredicates normalizeForPlanner(@Nonnull CorrelationIdentifier baseAlias) {
        return normalizeForPlanner(baseAlias, Collections.emptyList());
    }

    @API(API.Status.EXPERIMENTAL)
    ExpandedPredicates normalizeForPlanner(@Nonnull CorrelationIdentifier baseAlias, @Nonnull List<String> fieldNamePrefix);
}
