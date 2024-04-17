/*
 * PullUpValueRuleSet.java
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class PullUpValueRuleSet extends ValueComputationRuleSet<Iterable<? extends Value>, Map<Value, Function<Value, Value>>> {
    protected static final ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, ? extends Value> matchValueRule = new MatchValueRule();
    protected static final ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, ? extends Value> matchOrCompensateQuantifiedObjectValueRule = new MatchOrCompensateQuantifiedObjectValueRule();
    protected static final ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, ? extends Value> matchOrCompensateFieldValueRule = new MatchOrCompensateFieldValueRule();
    protected static final ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, ? extends Value> compensateRecordConstructorRule = new CompensateRecordConstructorRule();

    protected static final Set<ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, ? extends Value>> PULL_UP_RULES =
            ImmutableSet.of(matchValueRule,
                    matchOrCompensateQuantifiedObjectValueRule,
                    matchOrCompensateFieldValueRule,
                    compensateRecordConstructorRule);

    protected static final SetMultimap<ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, ? extends Value>, ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, ? extends Value>> PULL_UP_DEPENDS_ON =
            ImmutableSetMultimap.of();

    public PullUpValueRuleSet() {
        super(PULL_UP_RULES, PULL_UP_DEPENDS_ON);
    }

    public static PullUpValueRuleSet ofPullUpValueRules() {
        return new PullUpValueRuleSet();
    }
}
