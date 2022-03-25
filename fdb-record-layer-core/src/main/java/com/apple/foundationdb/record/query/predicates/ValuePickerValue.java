/*
 * ValuePickerValue.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A value representing multiple "alternative" values.
 * This value holds multiple "alternatives" for values and allows the creator to select one of them to be returned as
 * the "actual" value.
 * This is useful in cases (like {@link com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan}) where
 * there can be one of several plans that can be selected for execution, and this values gives visibility to all
 * of their {@link Value}s
 */
@API(API.Status.EXPERIMENTAL)
public class ValuePickerValue implements Value.CompileTimeValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Value-Picker-Value");

    private int selectedAlternative;
    private final List<? extends Value> alternativeValues;

    public ValuePickerValue(int selectedAlternative, @Nonnull final Iterable<? extends Value> alternativeValues) {
        List<? extends Value> values = ImmutableList.copyOf(alternativeValues);
        Verify.verify((selectedAlternative >= 0) && (selectedAlternative < values.size()));
        this.selectedAlternative = selectedAlternative;
        this.alternativeValues = values;
    }

    public ValuePickerValue(int selectedAlternative, @Nonnull final List<? extends Value> alternativeValues) {
        Verify.verify((selectedAlternative >= 0) && (selectedAlternative < alternativeValues.size()));
        this.selectedAlternative = selectedAlternative;
        this.alternativeValues = List.copyOf(alternativeValues);
    }

    public ValuePickerValue(int selectedAlternative, @Nonnull final Value... alternativeValues) {
        Verify.verify((selectedAlternative >= 0) && (selectedAlternative < alternativeValues.length));
        this.selectedAlternative = selectedAlternative;
        this.alternativeValues = List.of(alternativeValues);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return alternativeValues;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return alternativeValues.get(selectedAlternative).getResultType();
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        return new ValuePickerValue(selectedAlternative, newChildren);
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final AliasMap translationMap) {
        return this;
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return getSelectedValue().isFunctionallyDependentOn(otherValue);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, getChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getChildren());
    }

    @Override
    public String toString() {
        return "ValuePickerValue";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    private Value getSelectedValue() {
        return alternativeValues.get(selectedAlternative);
    }
}
