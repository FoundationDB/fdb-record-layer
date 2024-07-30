/*
 * JavaCallFunction.java
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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a Java user-defined function.
 */
@AutoService(BuiltInFunction.class)
public class JavaCallFunction extends BuiltInFunction<Value> {
    public JavaCallFunction() {
        super("java_call", List.of(Type.primitiveType(Type.TypeCode.STRING)), new Type.Any(), JavaCallFunction::findFunction);
    }

    @SuppressWarnings({"DataFlowIssue"})
    @Nonnull
    private static Value findFunction(@Nonnull final BuiltInFunction<Value> ignored, final List<? extends Typed> arguments) {
        Verify.verify(!arguments.isEmpty());
        Verify.verify(arguments.get(0).getResultType().getTypeCode().equals(Type.TypeCode.STRING));
        // dispatching happens at query-building time, therefore, the argument must be literal
        Verify.verify(arguments.get(0) instanceof LiteralValue<?>);
        final var literalValue = (LiteralValue<?>)arguments.get(0);
        final var functionName = (String)literalValue.compileTimeEval(null);

        // for now, the function name is expected to represent the fully-qualified class name, so we can find
        // it quickly via reflection, in the future we'll use the service loader to register the function.
        Class<?> clazz;
        try {
            clazz = Class.forName(functionName);
        } catch (ClassNotFoundException e) {
            throw new RecordCoreException("could not find function", e).addLogInfo(LogMessageKeys.FUNCTION, functionName);
        }
        // sanity / security check
        if (!UdfFunction.class.isAssignableFrom(clazz)) {
            throw new RecordCoreException("expecting class to be a subclass of '" + UdfFunction.class.getSimpleName() + "'")
                    .addLogInfo(LogMessageKeys.EXPECTED, UdfFunction.class.getSimpleName())
                    .addLogInfo(LogMessageKeys.ACTUAL, clazz.getName());
        }

        // the class must have parameterless constructor
        try {
            final Constructor<?> constructor = clazz.getDeclaredConstructor();
            return (Value)((UdfFunction)constructor.newInstance()).encapsulate(arguments.stream().skip(1).collect(Collectors.toUnmodifiableList()));
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RecordCoreException("could not instantiate call-site from '" + clazz.getName() + "'", e);
        }
    }
}
