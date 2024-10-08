/*
 * ExtractFromIndexKeyValueRuleSet.java
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

/**
 * A set of rules to be used to create a {@link Value} tree to extract an item from an index entry and perform
 * necessary adjustments prior to constructing the partial record in
 * {@link com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord}.
 * See also {@link com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.FieldWithValueCopier}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("java:S1452")
public class ExtractFromIndexKeyValueRuleSet extends ValueComputationRuleSet<Value, Map<Value, ValueCompensation>> {
    protected static final ValueComputationRule<Value, Map<Value, ValueCompensation>, ? extends Value> matchSimpleFieldValueRule = new MatchSimpleFieldValueRule();
    protected static final ValueComputationRule<Value, Map<Value, ValueCompensation>, ? extends Value> matchFieldValueOverFieldValueRule = new MatchFieldValueOverFieldValueRule();
    protected static final ValueComputationRule<Value, Map<Value, ValueCompensation>, ? extends Value> compensateToOrderedBytesValueRule = new CompensateToOrderedBytesValueRule();

    protected static final Set<ValueComputationRule<Value, Map<Value, ValueCompensation>, ? extends Value>> RULES =
            ImmutableSet.of(matchSimpleFieldValueRule,
                    matchFieldValueOverFieldValueRule,
                    compensateToOrderedBytesValueRule);

    protected static final SetMultimap<ValueComputationRule<Value, Map<Value, ValueCompensation>, ? extends Value>, ValueComputationRule<Value, Map<Value, ValueCompensation>, ? extends Value>> DEPENDS_ON = ImmutableSetMultimap.of();

    public ExtractFromIndexKeyValueRuleSet() {
        super(RULES, DEPENDS_ON);
    }

    public static ExtractFromIndexKeyValueRuleSet ofIndexKeyToPartialRecordValueRules() {
        return new ExtractFromIndexKeyValueRuleSet();
    }
}
