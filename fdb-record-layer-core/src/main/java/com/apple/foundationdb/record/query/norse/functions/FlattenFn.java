/*
 * WhereFn.java
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
import com.apple.foundationdb.record.query.norse.SemanticException;
import com.apple.foundationdb.record.query.predicates.Atom;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.Values;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Function
 * flatten(ANY*) -> TUPLE.
 */
@AutoService(BuiltInFunction.class)
public class FlattenFn extends BuiltInFunction<Value> {
    public FlattenFn() {
        super("flatten",
                ImmutableList.of(), new Type.Any(), FlattenFn::encapsulate);
    }

    @SuppressWarnings("UnstableApiUsage")
    public static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Atom> arguments) {
        SemanticException.check(!arguments.isEmpty(), "flatten needs at least one argument");
        SemanticException.check(arguments.stream().allMatch(argument -> argument.getResultType().getTypeCode() != Type.TypeCode.STREAM && argument instanceof Value),
                "flatten cannot be called to flatten streams");


        final ImmutableList<? extends Value> childrenAndNames = arguments.stream()
                .map(atom -> (Value)atom)
                .flatMap(value -> {
                    final Type resultType = value.getResultType();
                    SemanticException.check(resultType.getTypeCode() == Type.TypeCode.TUPLE, "arguments to flatten must be of type tuple");
                    return Type.Record.deconstructTuple(value).stream();
                })
                .collect(ImmutableList.toImmutableList());
        return Values.mergeTuples(childrenAndNames);
    }
}
