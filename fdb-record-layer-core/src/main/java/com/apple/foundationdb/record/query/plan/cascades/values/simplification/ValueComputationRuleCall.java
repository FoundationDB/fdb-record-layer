/*
 * ValueComputationRuleCall.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;

import org.jspecify.annotations.Nullable;
import java.util.Set;
import java.util.function.Function;

/**
 * A rule call implementation for the computation of a result while traversing {@link Value} trees.
 * @param <ARGUMENT> the type of the arguments to this rule call
 * @param <RESULT> the type of result this rule call produces
 */
@API(API.Status.EXPERIMENTAL)
public class ValueComputationRuleCall<ARGUMENT, RESULT> extends AbstractValueRuleCall<NonnullPair<Value, RESULT>, ValueComputationRuleCall<ARGUMENT, RESULT>> {

    @Nullable
    private final ARGUMENT argument;

    private final Function<Value, NonnullPair<Value, RESULT>> retrieveResultFunction;

    public ValueComputationRuleCall(final PlannerRule<ValueComputationRuleCall<ARGUMENT, RESULT>, ? extends Value> rule,
                                    final Value root,
                                    final Value current,
                                    final EvaluationContext evaluationContext,
                                    @Nullable final ARGUMENT argument,
                                    final PlannerBindings bindings,
                                    final AliasMap aliasMap,
                                    final Set<CorrelationIdentifier> constantAliases,
                                    final Function<Value, @Nullable QueryPlanConstraint> retrieveQueryPlanConstraintFunction,
                                    final Function<Value, NonnullPair<Value, RESULT>> retrieveResultFunction) {
        super(rule, root, current, evaluationContext, bindings, aliasMap, constantAliases,
                retrieveQueryPlanConstraintFunction);
        this.argument = argument;
        this.retrieveResultFunction = retrieveResultFunction;
    }

    @Nullable
    public ARGUMENT getArgument() {
        return argument;
    }

    @Nullable
    public NonnullPair<Value, RESULT> getResult(final Value value) {
        return retrieveResultFunction.apply(value);
    }

    public void yieldValue(final Value value, final RESULT result) {
        super.yieldResult(NonnullPair.of(value, result));
    }

    public ValueSimplificationRuleCall toValueSimplificationRuleCall(final AbstractRule<Value, ValueSimplificationRuleCall, Value, ? extends Value> rule) {
        return new ValueSimplificationRuleCall(rule, getRoot(), getCurrent(), getEvaluationContext(), getBindings(),
                getEquivalenceMap(), getConstantAliases(), getRetrieveQueryPlanConstraintFunction());
    }
}
