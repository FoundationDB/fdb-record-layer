/*
 * AbstractValueSimplificationRuleCall.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.Function;

/**
 * A rule call implementation for the computation of a result while traversing {@link Value} trees.
 * @param <R> the type of result this rule call produces
 */
@API(API.Status.EXPERIMENTAL)
public class ValueComputationRuleCall<R> extends AbstractValueRuleCall<ValueComputationRuleCall.ValueWithResult<R>, ValueComputationRuleCall<R>> {

    @Nonnull
    private final Function<Value, ValueWithResult<R>> retrieveResultFunction;

    public ValueComputationRuleCall(@Nonnull final AbstractValueRule<ValueWithResult<R>, ValueComputationRuleCall<R>, ? extends Value> rule,
                                    @Nonnull final Value root,
                                    @Nonnull final PlannerBindings bindings,
                                    @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                    @Nonnull final Function<Value, ValueWithResult<R>> retrieveResultFunction) {
        super(rule, root, bindings, constantAliases);
        this.retrieveResultFunction = retrieveResultFunction;
    }

    @Nonnull
    public ValueWithResult<R> getResult(@Nonnull final Value value) {
        return retrieveResultFunction.apply(value);
    }

    public void yield(@Nonnull final Value value, @Nonnull final R result) {
        super.yield(new ValueWithResult<>(value, result));
    }

    /**
     * Holder for a simplified value together with whatever result the simplification produced.
     * @param <R> the type of the result
     */
    public static class ValueWithResult<R> {
        @Nonnull
        private final Value value;
        @Nonnull
        private final R result;

        public ValueWithResult(@Nonnull final Value value, @Nonnull final R result) {
            this.value = value;
            this.result = result;
        }

        @Nonnull
        public Value getValue() {
            return value;
        }

        @Nonnull
        public R getResult() {
            return result;
        }
    }
}
