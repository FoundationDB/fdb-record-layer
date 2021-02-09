/*
 * PlanContext.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A basic context object that stores all of the metadata about a record store, such as the available indexes.
 * It provides access to this information to the planner and the
 * {@link PlannerRule#onMatch(PlannerRuleCall)} method.
 */
@API(API.Status.EXPERIMENTAL)
public interface PlanContext {
    @Nonnull
    Set<String> getRecordTypes();

    @Nonnull
    Set<Index> getIndexes();

    @Nonnull
    Index getIndexByName(@Nonnull String name);

    @Nullable
    KeyExpression getCommonPrimaryKey();

    int getGreatestPrimaryKeyWidth();

    @Nonnull
    RecordMetaData getMetaData();

    @Nonnull
    Set<MatchCandidate> getMatchCandidates();
}
