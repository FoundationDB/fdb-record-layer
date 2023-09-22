/*
 * JavaUserDefinedFunction.java
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
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Represents a Java user-defined function.
 */
@AutoService(BuiltInFunction.class)
public class JavaUserDefinedFunction extends BuiltInFunction<Value> {
    public JavaUserDefinedFunction() {
        super("java_call", List.of(Type.primitiveType(Type.TypeCode.STRING)), new Type.Any(), JavaUserDefinedFunction::findFunction);
    }

    @SuppressWarnings({"unchecked", "DataFlowIssue"})
    @Nonnull
    private static Value findFunction(@Nonnull final BuiltInFunction<Value> builtInFunction, final List<? extends Typed> arguments) {
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
            throw new RecordCoreException(String.format("could not find function '%s'", functionName), e);
        }
        // sanity / security check
        if (!JavaUdf.class.isAssignableFrom(clazz)) {
            throw new RecordCoreException(String.format("expecting '%s' to be a subclass of '%s'", clazz.getName(), JavaUdf.class.getName()));
        }

        final var fClazz = Verify.verifyNotNull(clazz);
        // @param arguments The types of the arguments, must be the same type or of a promotable type of the corresponding subclass expected {@link Value} parameter(s).
        Method method;
        try {
            method = fClazz.getMethod("getUdfParameterTypes");
        } catch (NoSuchMethodException e) {
            throw new RecordCoreException(String.format("could not find method '%s' in '%s'", "getUdfParameterTypes", clazz.getName()), e);
        }

        final List<Type> parameterTypes;
        try {
            parameterTypes = (List<Type>)method.invoke(null);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RecordCoreException(String.format("could not access method '%s' in '%s'", "getUdfParameterTypes", clazz.getName()), e);
        }

        // arguments[0] is the function name
        // arguments[1..n] are the function arguments
        if (arguments.size() - 1 != parameterTypes.size()) {
            throw new RecordCoreException("attempt to call '%s'.'%s' with incorrect number of parameters", clazz.getName(), "call");
        }

        final ImmutableList.Builder<Value> promotedArgumentsList = ImmutableList.builder();
        boolean argumentsChanged = false;

        for (int i = 1 ; i < arguments.size(); i++) {
            final var argument = arguments.get(i);
            final var parameter = parameterTypes.get(i - 1);
            final var maxType = Type.maximumType(argument.getResultType(), parameter);
            // Incompatible types
            SemanticException.check(maxType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            if (!argument.getResultType().equals(maxType)) {
                promotedArgumentsList.add(PromoteValue.inject((Value)argument, maxType));
                argumentsChanged = true;
            } else {
                promotedArgumentsList.add((Value)argument);
            }
        }

        final Constructor<?> constructor;
        try {
            constructor = fClazz.getConstructor(Iterable.class);
        } catch (NoSuchMethodException e) {
            throw new RecordCoreException(String.format("could not find a matching constructor in '%s'", fClazz.getName()), e);
        }
        try {
            if (argumentsChanged) {
                return (Value)constructor.newInstance(promotedArgumentsList.build());
            } else {
                return (Value)constructor.newInstance(arguments);
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RecordCoreException("could not access constructor '%s' in '%s'", constructor.getName(), fClazz.getName(), e);
        }
    }
}
