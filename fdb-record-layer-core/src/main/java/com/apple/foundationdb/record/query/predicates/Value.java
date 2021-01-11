/*
 * Value.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.Correlated;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A scalar value type.
 */
@API(API.Status.EXPERIMENTAL)
public interface Value extends Correlated<Value>, PlanHashable {

    @Nullable
    <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable FDBRecord<M> record, @Nullable M message);

    /**
     * Method to create a {@link QueryPredicate} that is based on this value and a
     * {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} that is passed in by the
     * caller.
     * @param comparison comparison to relate this value to
     * @return a new {@link ValuePredicate} using the passed in {@code comparison}
     */
    @Nonnull
    default ValuePredicate withComparison(@Nonnull Comparisons.Comparison comparison) {
        return new ValuePredicate(this, comparison);
    }

    /**
     * Method to create a {@link Placeholder} that is based on this value. A placeholder is also a {@link QueryPredicate}
     * that is used solely for matching query predicates against.
     * @param parameterAlias alias to uniquely identify the parameter in the
     *        {@link com.apple.foundationdb.record.query.plan.temp.MatchCandidate} this placeholder will be a part of.
     * @return a new {@link Placeholder}
     */
    @Nonnull
    default Placeholder asPlaceholder(@Nonnull final CorrelationIdentifier parameterAlias) {
        return ValueComparisonRangePredicate.placeholder(this, parameterAlias);
    }
}
