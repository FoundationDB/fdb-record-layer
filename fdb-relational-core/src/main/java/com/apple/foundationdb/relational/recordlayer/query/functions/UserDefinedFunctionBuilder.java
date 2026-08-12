/*
 * UserDefinedFunctionBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedMacroFunction;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Literals;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * A step builder that instantiates a {@link UserDefinedFunction} from the function definition specified in
 * a {@code CREATE FUNCTION} DDL statement.
 * <p>
 * Construction is split into a fixed sequence of steps, each modeled as its own interface which returns the next
 * builder step interface. This is to make the compiler enforce which set of options can be provided together and in
 * what order instead of relying on runtime checks. The builder steps are as follows:
 * <ol>
 *     <li>{@link SignatureStepBuilder}: specify the function signature
 *     ({@linkplain SignatureStepBuilder#setName(String) name}, {@linkplain SignatureStepBuilder#addParameter(Expression)
 *     parameters} and {@linkplain SignatureStepBuilder#setReturnType(DataType) optional return type}), then proceed to
 *     the next builder step via the method {@link SignatureStepBuilder#seal()}.</li>
 *     <li>{@link BodyStepBuilder}: supply the function body. The kind of body selects the kind of function produced by
 *     the final builder step: a relational expression body ({@link BodyStepBuilder#withBodyExpression(RelationalExpression)})
 *     yields a table-valued {@link CompiledSqlFunction}, whereas a scalar body ({@link BodyStepBuilder#withBodyValue(Value)})
 *     yields a {@link UserDefinedMacroFunction}.</li>
 *     <li>{@link FinalStepBuilder}: the builder step returned after specifying the function body, which applies any
 *     remaining optional specification and instantiates the correct instance of {@link UserDefinedFunction}.</li>
 * </ol>
 * </p>
 */
public final class UserDefinedFunctionBuilder {
    private UserDefinedFunctionBuilder() {
    }

    /**
     * Creates a new builder, positioned at its first ({@link SignatureStepBuilder}) step.
     * @return a builder ready to receive the function signature.
     */
    public static SignatureStepBuilder newBuilder() {
        return new UserDefinedFunctionSignatureStepBuilder();
    }

    /**
     * First builder step which specifies the function signature (name, parameters and optional return type).
     */
    public interface SignatureStepBuilder {
        /**
         * Sets the name of the function.
         * @param name the function name.
         * @return this builder, for chaining.
         */
        SignatureStepBuilder setName(@Nonnull String name);

        /**
         * Appends a single parameter to the signature.
         * <p>
         * The parameter carries its name, type and, optionally, a default value within the supplied {@link Expression};
         * a parameter whose underlying value is a {@link ThrowsValue} is taken to have no default.
         * </p>
         * <p>
         * Note: all parameter expressions must have a name, as unnamed parameters are not yet supported.
         * </p>
         * @param expression the parameter to append.
         * @return this builder, for chaining.
         */
        SignatureStepBuilder addParameter(@Nonnull Expression expression);

        /**
         * Appends all the given parameters to the signature, preserving their order.
         * <p>
         * The parameters carry their names, types and, optionally, a default value within the {@link Expression}; a
         * parameter expression whose underlying value is a {@link ThrowsValue} is taken to have no default.
         * </p>
         * <p>
         * Note: all parameter expressions must have a name, as unnamed parameters are not yet supported.
         * </p>
         * @param expressions the parameters to append.
         * @return this builder, for chaining.
         */
        SignatureStepBuilder addAllParameters(@Nonnull Expressions expressions);

        /**
         * Sets an explicit return type for the function; passing {@code null} leaves the return type to be inferred
         * from the function body.
         * @param returnType the explicit return type, or {@code null} to infer it from the body.
         * @return this builder, for chaining.
         */
        SignatureStepBuilder setReturnType(@Nullable DataType returnType);

        /**
         * Freezes the signature and advances to the {@linkplain BodyStepBuilder body step}.
         * @return the next step, ready to receive the function body.
         */
        BodyStepBuilder seal();
    }

    /**
     * Second step: supplies the function body, once the signature has been {@linkplain SignatureStepBuilder#seal() sealed}.
     */
    public interface BodyStepBuilder {
        /**
         * Returns a quantifier ranging over the function parameters, so the caller can use it to reference the
         * parameters while parsing the function body.
         * @return a quantifier over the parameters, or {@link Optional#empty()} if the function has none.
         */
        Optional<Quantifier> getParametersCorrelation();

        /**
         * Supplies a {@link RelationalExpression} as the function body, which returns a {@link FinalStepBuilder} that
         * eventually instantiates a {@link CompiledSqlFunction}.
         * @param bodyExpression the relational expression the function evaluates to.
         * @return the final step, which builds a {@link CompiledSqlFunction}.
         */
        FinalStepBuilder withBodyExpression(@Nonnull RelationalExpression bodyExpression);

        /**
         * Supplies a {@link Value} as the function body, which returns a {@link FinalStepBuilder} that
         * eventually instantiates a {@link UserDefinedMacroFunction}.
         * @param bodyValue the value the function evaluates to.
         * @return the final step, which builds a {@link UserDefinedMacroFunction}.
         */
        FinalStepBuilder withBodyValue(@Nonnull Value bodyValue);
    }

    /**
     * Final step: applies any remaining function specifications and instantiates the {@link UserDefinedFunction}.
     */
    public interface FinalStepBuilder {
        /**
         * Attaches the auxiliary literals gathered while parsing the body.
         * @param literals the literals to attach.
         * @return this builder, for chaining.
         */
        FinalStepBuilder setLiterals(@Nonnull Literals literals);

        /**
         * Builds the {@link UserDefinedFunction} from all the provided specifications.
         * @return the newly constructed function.
         */
        UserDefinedFunction build();
    }

    /**
     * Concrete implementation of the first two steps, {@link SignatureStepBuilder} and {@link BodyStepBuilder}. It
     * accumulates the signature and, once the body is supplied, hands off to the {@link FinalStepBuilder} that
     * matches the chosen body kind.
     */
    public static final class UserDefinedFunctionSignatureStepBuilder implements SignatureStepBuilder, BodyStepBuilder {
        @Nonnull
        private final ImmutableList.Builder<Expression> parametersBuilder;
        private String name;
        private Expressions parameters;
        private Quantifier.ForEach parametersQuantifier;
        private Type returnType;

        private UserDefinedFunctionSignatureStepBuilder() {
            this.parametersBuilder = ImmutableList.builder();
        }

        @Override
        public SignatureStepBuilder setName(@Nonnull final String name) {
            this.name = name;
            return this;
        }

        @Override
        public SignatureStepBuilder addParameter(@Nonnull final Expression expression) {
            parametersBuilder.add(expression.toNamedArgument());
            return this;
        }

        @Override
        public SignatureStepBuilder addAllParameters(@Nonnull final Expressions expressions) {
            parametersBuilder.addAll(expressions.asNamedArguments());
            return this;
        }

        @Override
        public SignatureStepBuilder setReturnType(@Nullable final DataType returnType) {
            if (returnType == null) {
                return this;
            }
            this.returnType = DataTypeUtils.toRecordLayerType(returnType);
            return this;
        }

        @Override
        public BodyStepBuilder seal() {
            Assert.notNullUnchecked(name);
            parameters = Expressions.of(parametersBuilder.build());
            // TODO: support unnamed parameters.
            Assert.thatUnchecked(parameters.allNamedArguments() /*|| parameters.allUnnamed()*/, ErrorCode.UNSUPPORTED_OPERATION,
                    "unnamed arguments is not supported");
            return this;
        }

        @Override
        public Optional<Quantifier> getParametersCorrelation() {
            Assert.notNullUnchecked(parameters);

            if (parameters.isEmpty()) {
                return Optional.empty();
            }
            if (parametersQuantifier == null) {
                final var select = GraphExpansion.builder()
                        .addAllResultColumns(parameters.underlyingAsColumns())
                        .build().buildSelect();
                parametersQuantifier = Quantifier.forEach(Reference.initialOf(select));
            }
            return Optional.of(parametersQuantifier);
        }

        @Override
        public FinalStepBuilder withBodyExpression(@Nonnull final RelationalExpression bodyExpression) {
            Assert.notNullUnchecked(name);
            Assert.notNullUnchecked(parameters);
            return new CompiledSqlFunction.CompiledSQLFunctionStepBuilder(
                    name, bodyExpression, parameters, getParameterDefaultValues(), parametersQuantifier, returnType);
        }

        @Override
        public FinalStepBuilder withBodyValue(@Nonnull final Value bodyValue) {
            Assert.notNullUnchecked(name);
            Assert.notNullUnchecked(parameters);
            return new UserDefinedMacroFunctionBuilder(name, bodyValue, parameters, getParameterDefaultValues(), parametersQuantifier, returnType);
        }

        private List<Optional<Value>> getParameterDefaultValues() {
            return Streams.stream(parameters.underlying())
                    .map(v -> v instanceof ThrowsValue ? Optional.<Value>empty() : Optional.of(v))
                    .collect(ImmutableList.toImmutableList());
        }
    }
}
