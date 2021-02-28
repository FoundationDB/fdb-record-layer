/*
 * RecordQuerySetOperationPlan.java
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * Interface for query plans that have fields that must be present for successful execution.
 */
public interface RecordQuerySetOperationPlan extends RecordQueryPlan {
    @Nonnull
    Set<KeyExpression> getRequiredFields();

    @Nonnull
    default List<? extends Value> getRequiredValues(@Nonnull final CorrelationIdentifier baseAlias) {
        return getRequiredFields()
                .stream()
                .map(keyExpression -> new ScalarTranslationVisitor(keyExpression).toResultValue(baseAlias))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    PushValueFunction pushValueFunction(final List<PushValueFunction> dependentFunctions);

    @Nonnull
    RecordQuerySetOperationPlan withChildren(@Nonnull final List<? extends RecordQueryPlan> newChildren);
}
