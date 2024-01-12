/*
 * RecordQueryFetchFromPartialRecordPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A query plan that transforms a stream of partial records (derived from index entries, as in the {@link RecordQueryCoveringIndexPlan})
 * into full records by fetching the records by primary key.
 */
@API(API.Status.INTERNAL)
public interface RecordPlanWithFetch extends RecordQueryPlan  {
    @Nonnull
    Optional<RecordQueryPlan> removeFetchMaybe();

    @Nonnull
    TranslateValueFunction getPushValueFunction();

    @Nonnull
    default Optional<Value> pushValueMaybe(@Nonnull Value value, @Nonnull CorrelationIdentifier sourceAlias, @Nonnull CorrelationIdentifier targetAlias) {
        return getPushValueFunction().translateValue(value, sourceAlias, targetAlias);
    }

    @Nonnull
    RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords getFetchIndexRecords();
}
