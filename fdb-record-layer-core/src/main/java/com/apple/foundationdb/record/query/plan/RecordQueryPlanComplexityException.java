/*
 * RecordQueryPlanComplexityException.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.explain.ExplainLevel;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;

/**
 * Exception thrown when a query plan is more complex than the configured limit.
 */
@SuppressWarnings("serial")
@API(API.Status.UNSTABLE)
public final class RecordQueryPlanComplexityException extends RecordCoreException {
    public RecordQueryPlanComplexityException(@Nonnull String msg) {
        super(msg);
    }

    public RecordQueryPlanComplexityException(RecordQueryPlan plan) {
        this("Plan exceeds complexity threshold");
        addLogInfo(LogMessageKeys.PLAN, ExplainPlanVisitor.toStringForExternalExplain(plan, ExplainLevel.ALL_DETAILS, 1_000));
    }
}
