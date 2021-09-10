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
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Typed;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

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

    private static RelationalExpression encapsulate(@Nonnull BuiltInFunction<RelationalExpression> builtInFunction, @Nonnull final List<Typed> arguments) {
        return null;
    }
}
