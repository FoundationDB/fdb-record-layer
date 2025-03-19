/*
 * MaxMatchMapSimplificationRuleSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
 * A set of rules for use by the maximum map computation logic. As the expressions reasoned over by the max match map
 * logic are already considered optimal with respect to simplification, the rules in this set do not further simplify
 * the value in the basic simplification sense. These rules transform the query side value into a different value, that
 * is not simpler but more <em>matchable</em> to the candidate side.
 * <br>
 * For instance, a query side value of {@code qov(q1)} is per se not matchable to
 * {@code rcv(fv(qov(q1), "b"), fv(qov(q1), "a"), fv(qov(q1), "c")}, however, expanding {@code qov(q1)} to
 * {@code rcv(fv(qov(q1), "a"), fv(qov(q1), "b"), fv(qov(q1), "c")} will create matches for
 * {@code rcv(fv(qov(q1), "a")}, {@code fv(qov(q1), "b")}, and {@code fv(qov(q1), "c")} respectively.
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

    private MaxMatchMapSimplificationRuleSet() {
        super(SIMPLIFICATION_RULES, SIMPLIFICATION_DEPENDS_ON);
    }

    public static MaxMatchMapSimplificationRuleSet instance() {
        return INSTANCE;
    }
}
