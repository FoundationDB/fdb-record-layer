/*
 * RecordQueryChooserPlanBase.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValuePickerValue;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link RecordQueryChooserPlanBase} is a plan that, at runtime, chooses to execute plans from multiple alternatives.
 * Subclasses of this base class will select, at runtime, one or more plans to execute from the list of optional children
 * held in this base class.
 * The invariant enforced by this base class is that all child plans are similar in the sense that they provide results
 * that are interchangeable: the same result set, in a compatible order (if one is specified).
 * Selecting one plan over the other will not have an impact on the client receiving the results.
 */
public abstract class RecordQueryChooserPlanBase implements RecordQueryPlanWithChildren {

    @Nonnull
    protected final List<Quantifier.Physical> quantifiers;
    private final boolean reverse;
    @Nonnull
    private final Supplier<List<? extends Value>> resultValuesSupplier;

    protected RecordQueryChooserPlanBase(@Nonnull final List<Quantifier.Physical> quantifiers) {
        Verify.verify(!quantifiers.isEmpty());
        this.quantifiers = List.copyOf(quantifiers);
        boolean firstReverse = quantifiers.get(0).getRangesOverPlan().isReverse();
        if (!getChildStream().allMatch(child -> child.isReverse() == firstReverse)) {
            throw new RecordCoreArgumentException("children of chooser plan do not all have same value for reverse field");
        }
        this.reverse = firstReverse;
        // Create a list of values that capture all the given sub-plans
        this.resultValuesSupplier = Suppliers.memoize(this::calculateChildrenValues);
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return quantifiers.stream().map(Quantifier.Physical::getRangesOverPlan).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValue() {
        return resultValuesSupplier.get();
    }

    @Nonnull
    protected Stream<RecordQueryPlan> getChildStream() {
        return quantifiers.stream().map(Quantifier.Physical::getRangesOverPlan);
    }

    @Nonnull
    protected RecordQueryPlan getChild(final int planIndex) {
        Verify.verify(quantifiers.size() > planIndex);
        return quantifiers.get(planIndex).getRangesOverPlan();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Set.of();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.intersection(getChildStream()
                .map(RecordQueryPlan::getAvailableFields)
                .collect(Collectors.toList()));
    }

    @Override
    public int getComplexity() {
        return 1 + getChildStream().mapToInt(RecordQueryPlan::getComplexity).sum();
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
    }

    @Override
    public int maxCardinality(@Nonnull RecordMetaData metaData) {
        return getChildStream().map(p -> p.maxCardinality(metaData)).min(Integer::compare).orElse(UNKNOWN_MAX_CARDINALITY);
    }

    @Override
    public boolean isStrictlySorted() {
        return getChildren().stream().allMatch(RecordQueryPlan::isStrictlySorted);
    }

    /**
     * This utility calculates the list of values that are returned by the plan. The plan returns a list of
     * {@link ValuePickerValue} that each represent the values returned by one of the child plans.
     * Each {@link ValuePickerValue} holds a "selected index" that determines which of the sub-values it references, so
     * that, in all, when the same "selected index" is chosen for all picker value, one would get back a consistent set
     * of values, representing one of the child plans for this plan.
     *
     * @return list of {@link ValuePickerValue} representing all the values from all the sub plans
     */
    private List<? extends Value> calculateChildrenValues() {
        // Store all values in a multimap, indexed by the ordinal of the value in the returned list
        // Each list represents all the i'th Value from each of the sub plans
        ImmutableListMultimap.Builder<Integer, Value> mapBuilder = ImmutableListMultimap.builder();
        quantifiers.forEach(quantifier -> {
            List<? extends Value> values = quantifier.getFlowedValues();
            for (int i = 0 ; i < values.size() ; i++) {
                mapBuilder.put(i, values.get(i));
            }
        });
        ImmutableListMultimap<Integer, ? extends Value> valuesMap = mapBuilder.build();

        ImmutableList.Builder<ValuePickerValue> resultBuilder = ImmutableList.builder();
        for (int i = 0 ; i < valuesMap.keySet().size() ; i++) {
            ImmutableList<? extends Value> subValues = valuesMap.get(i);
            // For now, fix all the picker values to return the first sub value
            resultBuilder.add(new ValuePickerValue(0, subValues));
        }

        return resultBuilder.build();
    }
}
