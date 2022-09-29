/*
 * FoldingValueSimplificationRuleSet.java
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

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A set of rules for simplifying {@link Value} trees used to expression ordering constraints.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class FoldingValueSimplificationRuleSet extends AbstractValueRuleSet<Value, ValueSimplificationRuleCall> {
    @Nonnull
    protected static final ValueSimplificationRule<? extends Value> foldConstantRule = new FoldConstantRule();

    private static final Set<ValueSimplificationRule<? extends Value>> FOLDING_SIMPLIFICATION_RULES =
            ImmutableSet.<ValueSimplificationRule<? extends Value>>builder()
                    .add(foldConstantRule)
                    .build();

    private static final SetMultimap<ValueSimplificationRule<? extends Value>, ValueSimplificationRule<? extends Value>> FOLDING_SIMPLIFICATION_DEPENDS_ON =
            ImmutableSetMultimap.of();

    private FoldingValueSimplificationRuleSet() {
        super(FOLDING_SIMPLIFICATION_RULES, FOLDING_SIMPLIFICATION_DEPENDS_ON);
    }

    public static FoldingValueSimplificationRuleSet ofFoldingSimplificationRules() {
        return new FoldingValueSimplificationRuleSet();
    }
}
