/*
 * UdfFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * This represents a user-defined function that can be subclassed to extend the planner with extra functionality.
 * Note: the default encapsulation logic does not support variadic functions.
 */
public abstract class HighOrderCompileTimeBuiltInFunction extends BuiltInFunction<Value> {

    public HighOrderCompileTimeBuiltInFunction() {
        super("", List.of(), (builtInFunction, arguments) -> null);
    }

    @Nonnull
    @Override
    public abstract List<Type> getParameterTypes();

    @Nonnull
    @Override
    public final String getFunctionName() {
        return this.getClass().getSimpleName();
    }

    @Nonnull
    protected abstract UdfValue newCallsite(@Nonnull List<Value> arguments);

    @Nonnull
    @Override
    public final Typed encapsulate(@Nonnull final List<? extends Typed> arguments) {
        arguments.forEach(argument -> Verify.verify(argument instanceof Value));
        final List<Type> parameterTypes = getParameterTypes();
        if (arguments.size() != parameterTypes.size()) {
            final String udfName = getFunctionName();
            throw new RecordCoreException("attempt to call " + udfName + " with incorrect number of parameters");
        }

        final ImmutableList.Builder<Value> promotedArgumentsList = ImmutableList.builder();

        for (int i = 0; i < arguments.size(); i++) {
            final var argument = arguments.get(i);
            final var parameter = parameterTypes.get(i);
            final var maxType = Type.maximumType(argument.getResultType(), parameter);
            // Incompatible types
            SemanticException.check(maxType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            if (!argument.getResultType().equals(maxType)) {
                promotedArgumentsList.add(PromoteValue.inject((Value)argument, maxType));
            } else {
                promotedArgumentsList.add((Value)argument);
            }
        }

        return newCallsite(promotedArgumentsList.build());
    }
}
