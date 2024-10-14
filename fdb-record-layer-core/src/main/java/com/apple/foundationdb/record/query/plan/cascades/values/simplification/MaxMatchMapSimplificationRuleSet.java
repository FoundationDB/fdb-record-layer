/*
 * DefaultValueSimplificationRuleSet.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for use by a planner that supports quickly finding rules that could match a given planner expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class MaxMatchMapSimplificationRuleSet extends AbstractValueRuleSet<Value, ValueSimplificationRuleCall> {
    @Nonnull
    protected static final ValueSimplificationRule<? extends Value> expandRecordRule = new ExpandRecordRule();
    @Nonnull
    protected static final ValueSimplificationRule<? extends FieldValue> expandFusedFieldValueRule = new ExpandFusedFieldValueRule();
    @Nonnull
    protected static final Set<ValueSimplificationRule<? extends Value>> SIMPLIFICATION_RULES =
            ImmutableSet.of(expandRecordRule, expandFusedFieldValueRule);
    @Nonnull
    protected static final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> SIMPLIFICATION_DEPENDS_ON =
            ImmutableSetMultimap.of();

    @Nonnull
    private static final MaxMatchMapSimplificationRuleSet INSTANCE = new MaxMatchMapSimplificationRuleSet();

    protected MaxMatchMapSimplificationRuleSet() {
        super(SIMPLIFICATION_RULES, SIMPLIFICATION_DEPENDS_ON);
    }

    public static MaxMatchMapSimplificationRuleSet instance() {
        return INSTANCE;
    }
}
