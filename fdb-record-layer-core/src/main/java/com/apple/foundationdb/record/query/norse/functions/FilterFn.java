/*
 * FilterFn.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.norse.functions;

import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.predicates.Lambda;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * Function
 * filter(RELATION, FUNCTION) -> RELATION.
 */
@AutoService(BuiltInFunction.class)
public class FilterFn extends BuiltInFunction<RelationalExpression> {
    public FilterFn() {
        super("filter",
                ImmutableList.of(new Type.Relation(), new Type.Function()), FilterFn::encapsulate);
    }

    private static RelationalExpression encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<RelationalExpression> builtInFunction, @Nonnull final List<Typed> arguments) {
        // the call is already validated against the resolved function
        Verify.verify(arguments.get(0) instanceof RelationalExpression);
        Verify.verify(arguments.get(1) instanceof Lambda);

        // get the typing information from the first argument
        final RelationalExpression inRelation = (RelationalExpression)arguments.get(0);
        final List<Type> columnTypes = Objects.requireNonNull(inRelation.getResultType().getColumnTypes(), "relation type must not be erased");

        // provide a calling scope to the lambda
        final Lambda lambda = (Lambda)arguments.get(1);

        final Quantifier.ForEach inQuantifier = Quantifier.forEachBuilder().build(GroupExpressionRef.of(inRelation));
        final List<? extends QuantifiedColumnValue> argumentValues = inQuantifier.getFlowedValues();
        final Typed filterTyped = lambda.unify(columnTypes, argumentValues);

        return new LogicalFilterExpression(ImmutableList.of(), inQuantifier);
    }
}
