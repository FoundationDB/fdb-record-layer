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

    /**
     * Method to derive if this value is functionally dependent on another value. In order to produce a meaningful
     * result that {@code otherValue} and this value must parts of the result values of the same
     * {@link com.apple.foundationdb.record.query.plan.temp.RelationalExpression}.
     *
     * <h2>Example 1</h2>
     * <pre>
     * {@code
     *    SELECT q, q.x, q.y
     *    FROM T q
     * }
     * </pre>
     * {@code q.x} and {@code q.y} are both functionally dependent on {@code q} meaning that for a given quantified
     * (bound) value of {@code q} there is exactly one scalar result for this value. In other words, {@code q -> q.x}
     * and {@code q -> q.y}
     *
     * <h2>Example 2</h2>
     * <pre>
     * {@code
     *    SELECT q1, q1.x, q2.y
     *    FROM S q1, T q2
     * }
     * </pre>
     * {@code q1.x} is functionally dependent on {@code q1} meaning that for a given quantified (bound) value of
     * {@code q1} there is exactly one scalar result for this value. In other words, {@code q1 -> q1.x}.
     * {@code q2.x} is functionally dependent on {@code q2} meaning that for a given quantified (bound) value of
     * {@code q2} there is exactly one scalar result for this value. In other words, {@code q2 -> q2.x}.
     * Note that it does not hold that {@code q1 -> q2.y} nor that {@code q2 -> q1.x}.
     *
     * <h2>Example 3</h2>
     * <pre>
     * {@code
     *    SELECT q1, q2.y
     *    FROM S q1, (SELECT * FROM EXPLODE(q1.x) q2
     * }
     * </pre>
     * {@code q2.y} is not functionally dependent on {@code q1} as for a given quantified (bound) value of {@code q1}
     * there may be many or no associated values over {@code q2}.
     *
     * <h2>Example 4</h2>
     * <pre>
     * {@code
     *    SELECT q1, 3
     *    FROM S q1, (SELECT * FROM EXPLODE(q1.x) q2
     * }
     * {@code 3} is functionally dependent on {@code q1} as for any given quantified (bound) value of {@code q1}
     * there is exactly one scalar result for this value (namely {@code 3}).*
     * </pre>
     *
     * Note that if {@code x -> y} and {@code y -> z} then {@code x -> z} should hold. Note that this method attempts
     * a best effort to establish the functional dependency relationship between the other value and this value. That
     * means that the caller cannot rely on a {@code false} result to deduce that this value is definitely not
     * dependent on {@code otherValue}.
     *
     * @param otherValue other value to check if this value is functionally dependent on it
     * @return {@code true} if this value is definitely dependent on {@code otherValue}
     */
    boolean isFunctionallyDependentOn(@Nonnull final Value otherValue);
}
