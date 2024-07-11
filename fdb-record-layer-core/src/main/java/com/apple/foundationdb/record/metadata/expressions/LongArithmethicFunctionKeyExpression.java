/*
 * ArithmethicFunctionKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FunctionCatalog;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * Function key expression representing evaluating arithmetic functions on {@code long}s.
 */
public class LongArithmethicFunctionKeyExpression extends FunctionKeyExpression implements QueryableKeyExpression {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Long-Arithmetic-Key-Expression-Function");

    @Nonnull
    private final String valueFunctionName;
    private final int minArguments;
    private final int maxArguments;

    @Nullable
    private final UnaryOperator<Long> unaryOperator;
    @Nullable
    private final BinaryOperator<Long> binaryOperator;

    private LongArithmethicFunctionKeyExpression(@Nonnull String name,
                                                 @Nonnull KeyExpression arguments,
                                                 @Nonnull String valueFunctionName,
                                                 int minArguments,
                                                 int maxArguments,
                                                 @Nullable UnaryOperator<Long> unaryOperator,
                                                 @Nullable BinaryOperator<Long> binaryOperator) {
        super(name, arguments);
        this.valueFunctionName = valueFunctionName;
        this.minArguments = minArguments;
        this.maxArguments = maxArguments;
        this.unaryOperator = unaryOperator;
        this.binaryOperator = binaryOperator;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return super.basePlanHash(hashMode, BASE_HASH);
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return super.baseQueryHash(hashKind, BASE_HASH);
    }

    @Override
    public int getMinArguments() {
        return minArguments;
    }

    @Override
    public int getMaxArguments() {
        return maxArguments;
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable final FDBRecord<M> record, @Nullable final Message message, @Nonnull final Key.Evaluated arguments) {
        Long result;
        if (arguments.size() == 1) {
            final Long x = arguments.getNullableLong(0);
            result = x == null ? null : Objects.requireNonNull(unaryOperator).apply(x);
        } else {
            final Long l = arguments.getNullableLong(0);
            final Long r = arguments.getNullableLong(1);
            result = (l == null || r == null) ? null : Objects.requireNonNull(binaryOperator).apply(l, r);
        }
        return ImmutableList.of(Key.Evaluated.scalar(result));
    }

    @Override
    public boolean createsDuplicates() {
        return arguments.createsDuplicates();
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    @Override
    public <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull final KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }

    @Nonnull
    @Override
    public Value toValue(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final Type baseType, @Nonnull final List<String> fieldNamePrefix) {
        ScalarTranslationVisitor scalarTranslationVisitor = new ScalarTranslationVisitor(arguments);
        scalarTranslationVisitor.push(ScalarTranslationVisitor.ScalarVisitorState.of(baseAlias, baseType, fieldNamePrefix));
        List<Value> argumentValues = new ArrayList<>(arguments.getColumnSize());
        for (KeyExpression expression : arguments.normalizeKeyForPositions()) {
            argumentValues.add(expression.expand(scalarTranslationVisitor));
        }
        BuiltInFunction<?> builtInFunction = FunctionCatalog.resolve(valueFunctionName, arguments.getColumnSize()).orElseThrow(() -> new RecordCoreArgumentException("unknown function", LogMessageKeys.FUNCTION, getName()));
        return (Value) builtInFunction.encapsulate(argumentValues);
    }

    /**
     * Builder for {@link LongArithmethicFunctionKeyExpression}s.
     */
    public static class Builder extends FunctionKeyExpression.Builder {
        private final String valueFunctionName;
        private final int minArguments;
        private final int maxArguments;
        @Nullable
        private final UnaryOperator<Long> unaryOperator;
        @Nullable
        private final BinaryOperator<Long> binaryOperator;

        private Builder(@Nonnull String functionName, @Nonnull String valueFunctionName, int minArguments, int maxArguments, @Nullable UnaryOperator<Long> unaryOperator, @Nullable BinaryOperator<Long> binaryOperator) {
            super(functionName);
            this.valueFunctionName = valueFunctionName;
            this.minArguments = minArguments;
            this.maxArguments = maxArguments;
            this.unaryOperator = unaryOperator;
            this.binaryOperator = binaryOperator;
        }

        @Nonnull
        @Override
        public FunctionKeyExpression build(@Nonnull final KeyExpression arguments) {
            return new LongArithmethicFunctionKeyExpression(functionName, arguments, valueFunctionName, minArguments, maxArguments, unaryOperator, binaryOperator);
        }

        /**
         * Create a new builder of a unary function. The resulting {@link LongArithmethicFunctionKeyExpression}
         * will only support operating on a single argument.
         *
         * @param name the name of the function
         * @param operator a lambda representing function execution
         * @return a new {@code Builder} of a unary arithmethic function expression
         */
        @Nonnull
        public static Builder unaryFunction(@Nonnull String name, @Nonnull UnaryOperator<Long> operator) {
            return unaryFunction(name, name, operator);
        }

        /**
         * Variant of {@link #unaryFunction(String, UnaryOperator)} that supports using an alternate name
         * for the {@link BuiltInFunction} used by the planner. By default, the value name will be assumed
         * to match the name of the function key expression.
         *
         * @param name the name of the function
         * @param valueFunctionName the name of the associated {@link BuiltInFunction}
         * @param operator a lambda representing function execution
         * @return a new {@link Builder} of a unary arithmetic function expression
         */
        @Nonnull
        public static Builder unaryFunction(@Nonnull String name, @Nonnull String valueFunctionName, @Nonnull UnaryOperator<Long> operator) {
            return new Builder(name, valueFunctionName, 1, 1, operator, null);
        }

        /**
         * Create a new builder of a binary function. The resulting {@link LongArithmethicFunctionKeyExpression}
         * will only support operating on exactly two arguments.
         *
         * @param name the name of the function
         * @param operator a lambda representing function execution
         * @return a new {@code Builder} of a binary arithmethic function expression
         */
        @Nonnull
        public static Builder binaryFunction(@Nonnull String name, @Nonnull BinaryOperator<Long> operator) {
            return binaryFunction(name, name, operator);
        }

        /**
         * Variant of {@link #binaryFunction(String, BinaryOperator)} that supports using an alternate name
         * for the {@link BuiltInFunction} used by the planner. By default, the value name will be assumed
         * to match the name of the function key expression.
         *
         * @param name the name of the function
         * @param valueFunctionName the name of the associated {@link BuiltInFunction}
         * @param operator a lambda representing function execution
         * @return a new {@link Builder} of a binary arithmetic function expression
         */
        @Nonnull
        public static Builder binaryFunction(@Nonnull String name, @Nonnull String valueFunctionName, @Nonnull BinaryOperator<Long> operator) {
            return new Builder(name, valueFunctionName, 2, 2, null, operator);
        }

        /**
         * Create a new builder of a function that can be either unary or binary.
         *
         * @param name the name of the function
         * @param unaryOperator the function to execute if a single argument is provided
         * @param binaryOperator the function to execute if two arguments are provided
         * @return a new {@code Builder} of a function that can be unary or binary
         */
        @Nonnull
        public static Builder bothFunction(@Nonnull String name, @Nonnull UnaryOperator<Long> unaryOperator, @Nonnull BinaryOperator<Long> binaryOperator) {
            return new Builder(name, name, 1, 2, unaryOperator, binaryOperator);
        }

        /**
         * Variant of {@link #bothFunction(String, UnaryOperator, BinaryOperator)} that supports using an alternate name
         * for the {@link BuiltInFunction} used by the planner. By default, the value name will be assumed
         * to match the name of the function key expression.
         *
         * @param name the name of the function
         * @param valueFunctionName the name of the associated {@link BuiltInFunction}
         * @param unaryOperator the function to execute if a single argument is provided
         * @param binaryOperator the function to execute if two arguments are provided
         * @return a new {@link Builder} of a function that can be unary or binary
         */
        @Nonnull
        public static Builder bothFunction(@Nonnull String name, @Nonnull String valueFunctionName, @Nonnull UnaryOperator<Long> unaryOperator, @Nonnull BinaryOperator<Long> binaryOperator) {
            return new Builder(name, valueFunctionName, 1, 2, unaryOperator, binaryOperator);
        }
    }

    /**
     * Factory for constructing built-in arithmetic functions that operate on {@code long}s.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class LongArithmethicFunctionKeyExpressionFactory implements FunctionKeyExpression.Factory {
        @Nonnull
        private static final List<FunctionKeyExpression.Builder> BUILDERS = ImmutableList.<FunctionKeyExpression.Builder>builder()
                .add(Builder.binaryFunction(FunctionNames.ADD, Math::addExact))
                .add(Builder.bothFunction("sub", x -> -x, Math::subtractExact))
                .add(Builder.bothFunction(FunctionNames.SUBTRACT, "sub", Math::negateExact, Math::subtractExact))
                .add(Builder.binaryFunction("mul", Math::multiplyExact))
                .add(Builder.binaryFunction(FunctionNames.MULTIPLY, "mul", Math::multiplyExact))
                .add(Builder.binaryFunction("div", (l, r) -> l / r))
                .add(Builder.binaryFunction(FunctionNames.DIVIDE, "div", (l, r) -> l / r))
                .add(Builder.binaryFunction(FunctionNames.MOD, (l, r) -> l % r))
                .add(Builder.binaryFunction(FunctionNames.BITOR, (l, r) -> l | r))
                .add(Builder.binaryFunction(FunctionNames.BITAND, (l, r) -> l & r))
                .add(Builder.binaryFunction(FunctionNames.BITXOR, (l, r) -> l ^ r))
                .add(Builder.unaryFunction(FunctionNames.BITNOT, x -> ~x))
                .build();

        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return BUILDERS;
        }
    }
}
