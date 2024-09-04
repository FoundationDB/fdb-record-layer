/*
 * DefaultOrderingPartRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * A rule that computes the default order of a {@link Value} tree. This rule is always applied last, that is when
 * no rule can make any more progress. It also refuses to make progress is some other order-simplifying rules has
 * already computed an order.
 * @param <O> type variable for sort order
 * @param <P> type variable for ordering part
 *
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class DefaultOrderingPartRule<O extends SortOrder, P extends OrderingPart<O>> extends ValueComputationRule<OrderingPartCreator<O, P>, P, Value> {
    @Nonnull
    private static final BindingMatcher<Value> rootMatcher = anyValue();

    @Nonnull
    private final O sortOrder;

    public DefaultOrderingPartRule(@Nonnull final O sortOrder) {
        super(rootMatcher);
        this.sortOrder = sortOrder;
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<OrderingPartCreator<O, P>, P> call) {
        final var bindings = call.getBindings();
        final var value = bindings.get(rootMatcher);
        final var orderingPartCreator = Objects.requireNonNull(call.getArgument());

        final var result = call.getResult(value);
        if (result == null) {
            call.yieldValue(value, orderingPartCreator.create(value, sortOrder));
        }
    }
}
