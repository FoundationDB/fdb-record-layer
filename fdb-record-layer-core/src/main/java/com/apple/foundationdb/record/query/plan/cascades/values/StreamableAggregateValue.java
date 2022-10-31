/*
 * StreamableAggregate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;

import javax.annotation.Nonnull;

/**
 * Signature interface used to mark {@link AggregateValue} implementations that can be calculated by a
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan} operator.
 * This is mainly used for matching.
 */
@API(API.Status.EXPERIMENTAL)
public interface StreamableAggregateValue extends AggregateValue, WithNamedOperator {
    /*
     * empty interface.
     */
    @Override
    default boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        return other.getClass() == getClass() && getOperatorName().equals(((StreamableAggregateValue)other).getOperatorName());
    }
}
