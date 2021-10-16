/*
 * MapFn.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.Atom;
import com.apple.foundationdb.record.query.predicates.Lambda;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.TupleConstructorValue;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * Function
 * map(STREAM, TUPLE -> TUPLE) -> STREAM.
 */
@AutoService(BuiltInFunction.class)
public class MapFn extends BuiltInFunction<RelationalExpression> {
    public MapFn() {
        super("map",
                ImmutableList.of(new Type.Stream(), new Type.Function(ImmutableList.of(Type.Record.erased()), Type.Record.erased())), MapFn::encapsulate);
    }

    public static RelationalExpression encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<RelationalExpression> builtInFunction, @Nonnull final List<Atom> arguments) {
        // the call is already validated against the resolved function
        Verify.verify(arguments.get(0) instanceof RecordQueryPlan);
        Verify.verify(arguments.get(1) instanceof Lambda);

        // get the typing information from the first argument
        final RecordQueryPlan inStream = (RecordQueryPlan)arguments.get(0);
        final Type streamedType = Objects.requireNonNull(inStream.getResultType().getInnerType(), "relation type must not be erased");
        Verify.verify(streamedType.getTypeCode() == Type.TypeCode.TUPLE);

        // provide a calling scope to the lambda
        final Lambda lambda = (Lambda)arguments.get(1);

        final Quantifier.Physical inQuantifier = Quantifier.physical(GroupExpressionRef.of(inStream));
        final List<? extends QuantifiedColumnValue> argumentValues = inQuantifier.getFlowedValues();
        final GraphExpansion graphExpansion = lambda.unifyBody(argumentValues);
        Verify.verify(graphExpansion.getQuantifiers().isEmpty());
        Verify.verify(graphExpansion.getPredicates().isEmpty());

        return new RecordQueryMapPlan(inQuantifier, TupleConstructorValue.tryUnwrapIfTuple(graphExpansion.getResultsAs(Value.class)));
    }
}
