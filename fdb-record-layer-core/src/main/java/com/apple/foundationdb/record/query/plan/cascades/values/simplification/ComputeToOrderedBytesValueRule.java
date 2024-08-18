/*
 * EliminateArithmeticValueWithConstantRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.OrderingPartCreator;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.SortOrder;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.TupleOrdering.Direction;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.toOrderedBytesValue;

/**
 * A rule that computes the underlying order of a {@link Value} tree rooted at a {@link ToOrderedBytesValue}.
 * @param <O> type variable for sort order
 * @param <P> type variable for ordering part
 *
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ComputeToOrderedBytesValueRule<O extends SortOrder, P extends OrderingPart<O>> extends ValueComputationRule<OrderingPartCreator<O, P>, P, ToOrderedBytesValue> {
    @Nonnull
    private static final CollectionMatcher<Value> childrenMatcher = all(anyValue());

    @Nonnull
    private static final BindingMatcher<ToOrderedBytesValue> rootMatcher =
            toOrderedBytesValue(childrenMatcher);

    @Nonnull
    private final Function<Direction, O> directionToSortOrderFunction;

    public ComputeToOrderedBytesValueRule(@Nonnull final Function<Direction, O> directionToSortOrderFunction) {
        super(rootMatcher);
        this.directionToSortOrderFunction = directionToSortOrderFunction;
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<OrderingPartCreator<O, P>, P> call) {
        final var bindings = call.getBindings();
        final var toOrderedBytesValue = bindings.get(rootMatcher);
        final var childrenCollection = bindings.get(childrenMatcher);
        final var onlyChild = Iterables.getOnlyElement(childrenCollection);
        final var orderingPartCreator = Objects.requireNonNull(call.getArgument());

        call.yieldValue(toOrderedBytesValue,
                orderingPartCreator.create(onlyChild, directionToSortOrderFunction.apply(toOrderedBytesValue.getDirection())));
    }
}
