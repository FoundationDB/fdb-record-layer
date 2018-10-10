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

import com.apple.foundationdb.record.metadata.Index;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A basic context object that stores all of the metadata about a record store, such as the available indexes.
 * It provides access to this information to the planner and the
 * {@link PlannerRule#onMatch(PlannerRuleCall)} method.
 */
public class PlanContext {
    @Nonnull
    private final Map<String, Index> indexes = new HashMap<>();

    public PlanContext(@Nonnull List<Index> indexes) {
        for (Index index : indexes) {
            this.indexes.put(index.getName(), index);
        }
    }

    @Nonnull
    public Map<String, Index> getIndexes() {
        return indexes;
    }

    @Nonnull
    public Optional<Index> getIndexByName(String name) {
        if (!indexes.containsKey(name)) {
            return Optional.empty();
        }
        return Optional.of(indexes.get(name));
    }
}
