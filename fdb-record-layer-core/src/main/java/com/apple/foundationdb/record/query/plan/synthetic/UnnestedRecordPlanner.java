/*
 * UnnestedRecordPlanner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.UnnestedRecordType;

import javax.annotation.Nonnull;

/**
 * Sub-planner used by the {@link SyntheticRecordPlanner} for constructing {@link UnnestedRecordType}s.
 */
@API(API.Status.INTERNAL)
class UnnestedRecordPlanner {
    @Nonnull
    private final UnnestedRecordType recordType;

    UnnestedRecordPlanner(@Nonnull UnnestedRecordType recordType) {
        this.recordType = recordType;
    }

    public SyntheticRecordFromStoredRecordPlan plan(UnnestedRecordType.NestedConstituent constituent) {
        if (!constituent.isParent()) {
            throw new RecordCoreArgumentException("Can only create synthetic records for parent constituent")
                    .addLogInfo(LogMessageKeys.EXPECTED, recordType.getParentConstituent().getName())
                    .addLogInfo(LogMessageKeys.ACTUAL, constituent.getName());
        }
        return new UnnestStoredRecordPlan(recordType, constituent.getRecordType());
    }
}
