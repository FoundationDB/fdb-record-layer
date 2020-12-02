/*
 * FakePlanContext.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A mock implementation of a {@link PlanContext} used to test certain planner rules that don't need a full plan context.
 */
public class FakePlanContext implements PlanContext {
    private final Map<String, Index> indexes;

    public FakePlanContext() {
        indexes = new TreeMap<>();
    }

    @Nonnull
    @Override
    public Set<Index> getIndexes() {
        return Sets.newHashSet(indexes.values());
    }

    @Nonnull
    @Override
    public Index getIndexByName(@Nonnull String name) {
        return indexes.get(name);
    }

    @Nullable
    @Override
    public KeyExpression getCommonPrimaryKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getGreatestPrimaryKeyWidth() {
        return 0;
    }

    @Nonnull
    @Override
    public RecordMetaData getMetaData() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Set<String> getRecordTypes() {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Set<MatchCandidate> getMatchCandidates() {
        return ImmutableSet.of();
    }
}
