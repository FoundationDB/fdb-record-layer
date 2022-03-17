/*
 * ExistsValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * A {@link Value} that checks whether an item exists in its child quantifier expression or not.
 */
@API(API.Status.EXPERIMENTAL)
public class ExistsValue implements BooleanValue, Value.CompileTimeValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Exists-Value");
    @Nonnull
    private final QuantifiedObjectValue child;

    public ExistsValue(@Nonnull QuantifiedObjectValue child) {
        this.child = child;
    }

    @Override
    @SuppressWarnings({"java:S2637", "ConstantConditions"}) // TODO the alternative component should not be null
    @SpotBugsSuppressWarnings("NP_NONNULL_PARAM_VIOLATION")
    public Optional<QueryPredicate> toQueryPredicate(@Nonnull final CorrelationIdentifier innermostAlias) {
        return Optional.of(new ExistsPredicate(child.getAlias(), null));
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return ImmutableList.of(child);
    }

    @Nonnull
    @Override
    public ExistsValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        final Value newChild = Iterables.getOnlyElement(newChildren);
        Verify.verify(newChild instanceof QuantifiedObjectValue);
        return new ExistsValue((QuantifiedObjectValue)newChild);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, child);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, child);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "exists(" + child.explain(formatter) + ")";
    }

    @Override
    public String toString() {
        return "exists(" + child + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    /**
     * A function that checks whether an item exists in a {@link RelationalExpression}.
     */
    @AutoService(BuiltInFunction.class)
    public static class ExistsFn extends BuiltInFunction<Value> {
        public ExistsFn() {
            super("exists",
                    ImmutableList.of(new Type.Relation()), (parserContext, builtInFunction, arguments) -> encapsulateInternal(parserContext, arguments));
        }

        private static Value encapsulateInternal(@Nonnull ParserContext parserContext, @Nonnull final List<Typed> arguments) {
            // the call is already validated against the resolved function
            Verify.verify(arguments.size() == 1);
            final Typed in = arguments.get(0);
            Verify.verify(in instanceof RelationalExpression);

            final GraphExpansion.Builder graphExpansionBuilder = parserContext.getCurrentScope().getGraphExpansionBuilder();

            // create an existential quantifier
            final Quantifier.Existential existsQuantifier = Quantifier.existential(GroupExpressionRef.of((RelationalExpression)in));
            graphExpansionBuilder.addQuantifier(existsQuantifier);

            return new ExistsValue(existsQuantifier.getFlowedObjectValue());
        }
    }
}
