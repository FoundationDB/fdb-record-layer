/*
 * AbstractValueRuleSet.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 * @param <R> the type that {@link AbstractValueRule}s in this set yield
 * @param <C> the type of the call rules in this set will receive
 *        when {@link com.apple.foundationdb.record.query.plan.cascades.PlannerRule#onMatch(PlannerRuleCall)} is invoked.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class AbstractValueRuleSet<R, C extends AbstractValueRuleCall<R, C>> extends AbstractRuleSet<C, Value> {
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    protected AbstractValueRuleSet(@Nonnull final Set<? extends AbstractValueRule<R, C, ? extends Value>> rules,
                                   @Nonnull final SetMultimap<? extends AbstractValueRule<R, C, ? extends Value>, ? extends AbstractValueRule<R, C, ? extends Value>> dependencies) {
        super(rules, dependencies);
    }
}
