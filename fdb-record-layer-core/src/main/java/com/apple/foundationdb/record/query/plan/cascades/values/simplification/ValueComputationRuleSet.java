/*
 * ValueComputationRuleSet.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 * @param <A> the type of argument that rules in this set consume
 * @param <R> the type of result that rules in this set (or subclasses thereof) produce
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public abstract class ValueComputationRuleSet<A, R> extends AbstractValueRuleSet<Pair<Value, R>, ValueComputationRuleCall<A, R>> {

    public ValueComputationRuleSet(@Nonnull final Set<? extends AbstractValueRule<Pair<Value, R>, ValueComputationRuleCall<A, R>, ? extends Value>> abstractValueSimplificationRules,
                                   @Nonnull final SetMultimap<? extends AbstractValueRule<Pair<Value, R>, ValueComputationRuleCall<A, R>, ? extends Value>, ? extends AbstractValueRule<Pair<Value, R>, ValueComputationRuleCall<A, R>, ? extends Value>> dependsOn) {
        super(abstractValueSimplificationRules, dependsOn);
    }
}
