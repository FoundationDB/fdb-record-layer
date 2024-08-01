/*
 * ValueComputationRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;

import javax.annotation.Nonnull;

/**
 * Intermediate class that fixes the type of the {@link com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall}.
 * @param <A> the type of object that functions as the argument to this rule
 * @param <R> the type of object that this rule produces as result
 * @param <T> the type of object that this rule helps simplify
 */
@API(API.Status.EXPERIMENTAL)
public abstract class ValueComputationRule<A, R, T extends Value> extends AbstractValueRule<NonnullPair<Value, R>, ValueComputationRuleCall<A, R>, T> {
    public ValueComputationRule(@Nonnull final BindingMatcher<T> matcher) {
        super(matcher);
    }
}
