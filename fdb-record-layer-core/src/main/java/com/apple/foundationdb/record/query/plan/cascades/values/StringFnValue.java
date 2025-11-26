/*
 * StringFnValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PStringFnValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * A {@link Value} that applies a string function on its child expression.
 */
@API(API.Status.EXPERIMENTAL)
public class StringFnValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("StringFn-Value");

    @Nonnull
    private final StringFn function;
    @Nonnull
    private final Value child;

    /**
     * Constructs a new instance of {@link StringFnValue}.
     * @param function The string function to apply.
     * @param child The child value.
     */
    public StringFnValue(@Nonnull final StringFn function, @Nonnull final Value child) {
        this.function = function;
        this.child = child;
    }

    @Nonnull
    public StringFn getFunction() {
        return function;
    }

    @Nonnull
    public Value getChild() {
        return child;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final Object childValue = child.eval(store, context);
        if (childValue == null) {
            return null;
        }
        if (!(childValue instanceof String)) {
            SemanticException.fail(SemanticException.ErrorCode.INCOMPATIBLE_TYPE,
                    "String function requires string argument");
        }
        return function.apply((String) childValue);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSupplier) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens()
                .addFunctionCall(function.getFunctionName(),
                        Value.explainFunctionArguments(explainSupplier)));
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(TypeCode.STRING);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(child);
    }

    @Nonnull
    @Override
    public StringFnValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        return new StringFnValue(this.function, Iterables.get(newChildren, 0));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, function);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, function, child);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        if (this == other) {
            return ConstrainedBoolean.alwaysTrue();
        }
        if (!(other instanceof StringFnValue)) {
            return ConstrainedBoolean.falseValue();
        }
        final StringFnValue that = (StringFnValue) other;
        return this.function == that.function ? ConstrainedBoolean.alwaysTrue() : ConstrainedBoolean.falseValue();
    }

    @Override
    public String toString() {
        return function.getFunctionName() + "(" + child + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PStringFnValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PStringFnValue.newBuilder()
                .setFunction(function.toProto(serializationContext))
                .setChild(child.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setStringFnValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static StringFnValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PStringFnValue stringFnValueProto) {
        return new StringFnValue(
                StringFn.fromProto(serializationContext, stringFnValueProto.getFunction()),
                Value.fromValueProto(serializationContext, stringFnValueProto.getChild()));
    }

    @Nonnull
    static Value encapsulate(@Nonnull final List<? extends Typed> arguments, @Nonnull final StringFn function) {
        Verify.verify(arguments.size() == 1);
        final Typed arg = arguments.get(0);
        Verify.verify(arg instanceof Value);
        final Value argValue = (Value) arg;

        // Validate that the argument is a string type
        final Type argType = argValue.getResultType();
        if (!argType.isUnresolved() && argType.getTypeCode() != TypeCode.STRING) {
            SemanticException.fail(SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                    "String function requires string argument, got " + argType);
        }

        return new StringFnValue(function, argValue);
    }

    /**
     * String function enum.
     */
    public enum StringFn implements PlanHashable {
        LOWER("lower") {
            @Override
            public String apply(@Nonnull final String input) {
                return input.toLowerCase(Locale.ROOT);
            }
        },
        UPPER("upper") {
            @Override
            public String apply(@Nonnull final String input) {
                return input.toUpperCase(Locale.ROOT);
            }
        };

        @Nonnull
        private final String functionName;

        StringFn(@Nonnull final String functionName) {
            this.functionName = functionName;
        }

        @Nonnull
        public String getFunctionName() {
            return functionName;
        }

        @Nonnull
        public abstract String apply(@Nonnull String input);

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return PlanHashable.objectsPlanHash(mode, BASE_HASH, functionName);
        }

        @Nonnull
        public PStringFnValue.PStringFn toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PStringFnValue.PStringFn.valueOf(this.name());
        }

        @Nonnull
        public static StringFn fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PStringFnValue.PStringFn stringFnProto) {
            return StringFn.valueOf(stringFnProto.name());
        }
    }

    /**
     * The {@code lower} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class LowerFn extends BuiltInFunction<Value> {
        public LowerFn() {
            super("lower",
                    ImmutableList.of(Type.primitiveType(TypeCode.STRING)),
                    (ignored, args) -> StringFnValue.encapsulate(args, StringFn.LOWER));
        }
    }

    /**
     * The {@code upper} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class UpperFn extends BuiltInFunction<Value> {
        public UpperFn() {
            super("upper",
                    ImmutableList.of(Type.primitiveType(TypeCode.STRING)),
                    (ignored, args) -> StringFnValue.encapsulate(args, StringFn.UPPER));
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PStringFnValue, StringFnValue> {
        @Nonnull
        @Override
        public Class<PStringFnValue> getProtoMessageClass() {
            return PStringFnValue.class;
        }

        @Nonnull
        @Override
        public StringFnValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PStringFnValue stringFnValueProto) {
            return StringFnValue.fromProto(serializationContext, stringFnValueProto);
        }
    }
}
