/*
 * StreamValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * a {@link Value} that returns a stream of results upon evaluation.
 */
public interface StreamingValue extends Value {

    /**
     * Returns a {@link RecordCursor} over the result stream returned upon evaluation.
     *
     * @param store The record store used to fetch and evaluate the results.
     * @param context The evaluation context, containing a set of contextual parameters for evaluating the results.
     * @param continuation The continuation bytes used to resume the evaluation of the result stream.
     * @param executeProperties The execution properties.
     * @param <M> The type of the returned results when fetched from a record store.
     * @return A cursor over the result stream returned upon evaluation.
     */
    @Nonnull
    <M extends Message> RecordCursor<QueryResult> evalAsStream(@Nonnull FDBRecordStoreBase<M> store,
                                                               @Nonnull EvaluationContext context,
                                                               @Nullable byte[] continuation,
                                                               @Nonnull ExecuteProperties executeProperties);

    /**
     * Get the cardinality bounds for this streaming value.
     *
     * @return a min and max cardinality bound for this value
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    CardinalitiesProperty.Cardinalities getCardinalities();

    /**
     * Whether this streaming value of a query implies a streaming value of a match candidate, that is, whether the
     * candidate's value produces everything this value may produce. By default, a value only implies a semantically
     * equal one.
     *
     * @param candidateValue the streaming value of the match candidate
     * @param bindingAliasMap a map of aliases defining the equivalence between quantifiers of the query and of the
     *        match candidate
     * @return whether this value implies the candidate's value
     */
    @API(API.Status.INTERNAL)
    default boolean impliesCandidateValue(@Nonnull final StreamingValue candidateValue,
                                          @Nonnull final AliasMap bindingAliasMap) {
        return semanticEquals(candidateValue, bindingAliasMap);
    }
}
