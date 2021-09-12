/*
 * ParserWalker.java
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

package com.apple.foundationdb.record.query.norse;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.Lambda;
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Type.TypeCode;
import com.apple.foundationdb.record.query.predicates.Typed;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.predicates.Type.primitiveType;

public class ParserWalker extends NorseParserBaseVisitor<Typed> {

    private final ParserContext parserContext;

    public ParserWalker(@Nonnull RecordMetaData recordMetaData, @Nonnull RecordStoreState recordStoreState) {
        this(new ParserContext(new Scopes(), recordMetaData, recordStoreState));
    }

    public ParserWalker(@Nonnull final ParserContext parserContext) {
        this.parserContext = parserContext;
    }

    @Override
    public Typed visitPipeMethodCall(final NorseParser.PipeMethodCallContext ctx) {
        final NorseParser.PipeContext pipeContext = Objects.requireNonNull(ctx.pipe());
        final NorseParser.MethodCallContext methodCallContext = Objects.requireNonNull(ctx.methodCall());
        final String functionName = Objects.requireNonNull(methodCallContext.IDENTIFIER()).getText();
        final int numberOfArguments = numberOfCallArguments(methodCallContext) + 1;
        final Optional<BuiltInFunction<? extends Typed>> functionOptional = FunctionCatalog.resolve(functionName, numberOfArguments);
        if (!functionOptional.isPresent()) {
            throw new IllegalArgumentException("unable to resolve function");
        }
        return functionOptional.flatMap(function -> {
            final ImmutableList.Builder<Typed> argumentsBuilder = ImmutableList.builder();
            final List<Type> resolvedParameterTypes = function.resolveParameterTypes(numberOfArguments);
            argumentsBuilder.add(callArgument(pipeContext, resolvedParameterTypes.get(0)));
            argumentsBuilder.addAll(callArguments(methodCallContext, resolvedParameterTypes.subList(1, resolvedParameterTypes.size())));
            final List<Typed> arguments = argumentsBuilder.build();
            return functionOptional
                    .flatMap(builtInFunction -> builtInFunction.validateCall(Type.fromTyped(arguments)))
                    .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments));
        }).orElseThrow(() -> new IllegalArgumentException("unable to compile in function"));
    }

    @Override
    public Typed visitExpressionField(final NorseParser.ExpressionFieldContext ctx) {
        final NorseParser.ExpressionContext expressionContext = Objects.requireNonNull(ctx.expression());
        final TerminalNode identifier = ctx.IDENTIFIER();

        final Typed typed = expressionContext.accept(this);
        if (typed.getResultType().getTypeCode() != TypeCode.RECORD) {
            throw new IllegalArgumentException("context to a field accessor has to be of type record");
        }
        Verify.verify(typed instanceof Value);
        Verify.verify(typed.getResultType() instanceof Type.Record);
        final Type.Record recordType = (Type.Record)typed.getResultType();
        final String fieldName = identifier.getText();
        final Map<String, Type> fieldTypeMap = Objects.requireNonNull(recordType.getFieldTypeMap());
        Preconditions.checkArgument(fieldTypeMap.containsKey(fieldName), "attempting to query non existing field");
        final Type fieldType = fieldTypeMap.get(fieldName);

        if (typed instanceof FieldValue) {
            ImmutableList.Builder<String> fieldPathBuilder = ImmutableList.builder();
            fieldPathBuilder.addAll(((FieldValue)typed).getFieldPath());
            fieldPathBuilder.add(fieldName);
            return new FieldValue(((FieldValue)typed).getChild(), fieldPathBuilder.build(), fieldType);
        } else if (typed instanceof QuantifiedColumnValue) {
            return new FieldValue((QuantifiedColumnValue)typed, ImmutableList.of(fieldName), fieldType);
        }

        // TODO
        throw new IllegalStateException("unable to support arbitrary context expressions");
    }

    @Override
    public Typed visitExpressionFunctionCall(final NorseParser.ExpressionFunctionCallContext ctx) {
        Objects.requireNonNull(ctx.functionCall());
        final NorseParser.MethodCallContext methodCallContext = Objects.requireNonNull(ctx.functionCall().methodCall());
        final String functionName = Objects.requireNonNull(methodCallContext.IDENTIFIER()).getText();
        final int numberOfArguments = numberOfCallArguments(methodCallContext);
        final Optional<BuiltInFunction<? extends Typed>> functionOptional = FunctionCatalog.resolve(functionName, numberOfArguments);
        if (!functionOptional.isPresent()) {
            throw new IllegalArgumentException("unable to resolve function");
        }

        return functionOptional.flatMap(function -> {
            final ImmutableList.Builder<Typed> argumentsBuilder = ImmutableList.builder();
            final List<Type> resolvedParameterTypes = function.resolveParameterTypes(numberOfArguments);
            argumentsBuilder.addAll(callArguments(methodCallContext, resolvedParameterTypes));
            final List<Typed> arguments = argumentsBuilder.build();
            return functionOptional
                    .flatMap(builtInFunction -> builtInFunction.validateCall(Type.fromTyped(arguments)))
                    .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments));
        }).orElseThrow(() -> new IllegalArgumentException("unable to compile in function"));
    }

    private int numberOfCallArguments(final NorseParser.MethodCallContext ctx) {
        if (ctx.expressionList() != null) {
            return ctx.expressionList().expression().size();
        } else if (ctx.expression() != null) {
            return 1;
        }
        throw new ParserSyncException(ctx, "unknown rule");
    }

    @Nonnull
    private List<Typed> callArguments(@Nonnull final NorseParser.MethodCallContext ctx, @Nonnull final List<Type> declaredParameterTypes) {
        if (ctx.expressionList() != null) {
            final NorseParser.ExpressionListContext expressionListContext = ctx.expressionList();

            final ImmutableList.Builder<Typed> resultBuilder = ImmutableList.builder();
            List<NorseParser.ExpressionContext> expression = expressionListContext.expression();
            for (int i = 0, expressionSize = expression.size(); i < expressionSize; i++) {
                resultBuilder.add(callArgument(expression.get(i), declaredParameterTypes.get(i)));
            }

            return resultBuilder.build();
        } else if (ctx.expression() != null) {
            return ImmutableList.of(ctx.expression().accept(this));
        }
        throw new ParserSyncException(ctx, "unknown rule");
    }

    private Typed callArgument(@Nonnull final ParserRuleContext expressionContext, @Nonnull final Type declaredParameterType) {
        if (declaredParameterType.getTypeCode() == TypeCode.FUNCTION &&
                !(expressionContext instanceof NorseParser.ExpressionLambdaContext)) {
            return fromLambdaBody(ImmutableList.of(), expressionContext);
        } else {
            return expressionContext.accept(this);
        }
    }

    @Override
    public Typed visitExpressionUnaryBang(final NorseParser.ExpressionUnaryBangContext ctx) {
        // visit the children expressions
        final ImmutableList<Typed> arguments =
                ImmutableList.of(ctx.expression().accept(this));

        final Optional<BuiltInFunction<? extends Typed>> functionOptional;
        if (ctx.BANG() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("not", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve not()"));
    }

    @Override
    public Typed visitExpressionInequality(final NorseParser.ExpressionInequalityContext ctx) {
        // visit the children expressions
        final ImmutableList<Typed> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Typed>> functionOptional;
        if (ctx.LT() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("lt", Type.fromTyped(arguments));
        } else if (ctx.LE() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("lte", Type.fromTyped(arguments));
        } else if (ctx.GT() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("gt", Type.fromTyped(arguments));
        } else if (ctx.GE() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("gte", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve comparators"));
    }

    @Override
    public Typed visitExpressionEqualityNonEquality(final NorseParser.ExpressionEqualityNonEqualityContext ctx) {
        // visit the children expressions
        final ImmutableList<Typed> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Typed>> functionOptional;
        if (ctx.EQUAL() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("equals", Type.fromTyped(arguments));
        } else if (ctx.NOTEQUAL() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("notEquals", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve comparators"));
    }

    @Override
    public Typed visitExpressionLogicalAnd(final NorseParser.ExpressionLogicalAndContext ctx) {
        // visit the children expressions
        final ImmutableList<Typed> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Typed>> functionOptional;
        if (ctx.AND() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("and", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve and()"));
    }

    @Override
    public Typed visitExpressionLogicalOr(final NorseParser.ExpressionLogicalOrContext ctx) {
        // visit the children expressions
        final ImmutableList<Typed> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Typed>> functionOptional;
        if (ctx.OR() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("or", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve or()"));
    }

    @Override
    public Typed visitExpressionLambda(final NorseParser.ExpressionLambdaContext ctx) {
        final NorseParser.LambdaContext lambdaContext = Objects.requireNonNull(ctx.lambda());
        final NorseParser.ExpressionContext expressionContext = Objects.requireNonNull(lambdaContext.expression());
        final NorseParser.LambdaParametersContext lambdaParametersContext = Objects.requireNonNull(lambdaContext.lambdaParameters());
        final List<Optional<String>> declaredParameterNames =
                Objects.requireNonNull(lambdaParametersContext.bindingIdentifier())
                        .stream()
                        .map(bindingIdentifier -> {
                            if (bindingIdentifier.IDENTIFIER() != null) {
                                return Optional.of(bindingIdentifier.IDENTIFIER().getText());
                            }
                            return Optional.<String>empty();
                        })
                        .collect(ImmutableList.toImmutableList());

        return fromLambdaBody(declaredParameterNames, expressionContext);
    }

    @Nonnull
    private Lambda fromLambdaBody(@Nonnull final List<Optional<String>> parameterNames,
                                  @Nonnull final ParserRuleContext parserRuleContext) {
        // save the current scope -- this is for captures
        final Scopes.Scope definingScope = parserContext.getCurrentScope();

        return new Lambda(parameterNames, (visibleAliases, boundIdentifiers) -> {
            final Scopes callingScopes =
                    new Scopes(definingScope)
                            .push(visibleAliases, boundIdentifiers);
            final ParserWalker nestedWalker = withScopes(callingScopes);

            // TODO stuff to check the arguments are properly bound to the declared parameters (by cardinality and later by
            //      (optionally) declared type

            // resolve and encapsulate now that the arguments should be properly provided by the caller
            return parserRuleContext.accept(nestedWalker);
        });
    }

    @Override
    public Typed visitPrimaryExpressionFromNestedPipe(final NorseParser.PrimaryExpressionFromNestedPipeContext ctx) {
        return super.visitPrimaryExpressionFromNestedPipe(ctx);
    }

    @Override
    public Typed visitPrimaryExpressionFromRecordConstructor(final NorseParser.PrimaryExpressionFromRecordConstructorContext ctx) {
        throw new UnsupportedOperationException("unable to construct record");
    }

    @Override
    public Typed visitPrimaryExpressionFromLiteral(final NorseParser.PrimaryExpressionFromLiteralContext ctx) {
        return super.visitPrimaryExpressionFromLiteral(ctx);
    }

    @Override
    public Typed visitPrimaryExpressionFromUnderbar(final NorseParser.PrimaryExpressionFromUnderbarContext ctx) {
        return parserContext.resolveIdentifier(ctx.UNDERBAR().getSymbol().getText());
    }

    @Override
    public Typed visitPrimaryExpressionFromIdentifier(final NorseParser.PrimaryExpressionFromIdentifierContext ctx) {
        return parserContext.resolveIdentifier(ctx.IDENTIFIER().getSymbol().getText());
    }

    @Override
    public Typed visitLiteral(final NorseParser.LiteralContext ctx) {
        final NorseParser.IntegerLiteralLongContext integerLiteralContextLong = ctx.integerLiteralLong();
        if (integerLiteralContextLong != null) {
            final Long result = unboxLiteral(visit(integerLiteralContextLong), Long.class);
            return new LiteralValue<>(primitiveType(TypeCode.LONG), result);
        }
        final NorseParser.IntegerLiteralContext integerLiteralContext = ctx.integerLiteral();
        if (integerLiteralContext != null) {
            final Integer result = unboxLiteral(visit(integerLiteralContext), Integer.class);
            return new LiteralValue<>(primitiveType(TypeCode.INT), result);
        }
        final NorseParser.FloatLiteralDoubleContext floatLiteralDoubleContext = ctx.floatLiteralDouble();
        if (floatLiteralDoubleContext != null) {
            final Double result = unboxLiteral(visit(floatLiteralDoubleContext), Double.class);
            return new LiteralValue<>(primitiveType(TypeCode.DOUBLE), result);
        }
        final NorseParser.FloatLiteralContext floatLiteralContext = ctx.floatLiteral();
        if (floatLiteralContext != null) {
            final Float result = unboxLiteral(visit(floatLiteralContext), Float.class);
            return new LiteralValue<>(primitiveType(TypeCode.FLOAT), result);
        }
        final TerminalNode stringLiteral = ctx.STRING_LITERAL();
        if (stringLiteral != null) {
            final String literalWithQuotes = stringLiteral.getSymbol().getText();
            return new LiteralValue<>(primitiveType(TypeCode.STRING), literalWithQuotes.substring(1, literalWithQuotes.length() - 1));
        }
        final TerminalNode booleanLiteral = ctx.BOOL_LITERAL();
        if (booleanLiteral != null) {
            return new LiteralValue<>(primitiveType(TypeCode.BOOLEAN), Boolean.valueOf(booleanLiteral.getSymbol().getText()));
        }
        if (ctx.NULL_LITERAL() != null) {
            return new LiteralValue<>(primitiveType(TypeCode.UNKNOWN), null);
        }

        throw new ParserSyncException(ctx, "unknown rule");
    }

    @Override
    public Typed visitIntegerLiteralLong(final NorseParser.IntegerLiteralLongContext ctx) {
        if (ctx.DECIMAL_LITERAL_LONG() != null) {
            final String text = ctx.DECIMAL_LITERAL_LONG().getSymbol().getText();
            // strip the "lL" bit at the end
            return new Typed.TypedLiteral(TypeCode.LONG, Long.parseLong(text.substring(0, text.length() - 1)));
        } else if (ctx.BINARY_LITERAL_LONG() != null ||
                   ctx.HEX_LITERAL_LONG() != null  ||
                   ctx.OCT_LITERAL_LONG() != null) {
            throw new UnsupportedOperationException("(binary | hex | oct) int literal not supported yet");
        }
        throw new ParserSyncException(ctx, "unknown int literal");
    }

    @Override
    public Typed visitIntegerLiteral(final NorseParser.IntegerLiteralContext ctx) {
        if (ctx.DECIMAL_LITERAL() != null) {
            return new Typed.TypedLiteral(TypeCode.INT, Integer.parseInt(ctx.DECIMAL_LITERAL().getSymbol().getText()));
        } else if (ctx.BINARY_LITERAL() != null ||
                   ctx.HEX_LITERAL() != null  ||
                   ctx.OCT_LITERAL() != null) {
            throw new UnsupportedOperationException("(binary | hex | oct) int literal not supported yet");
        }
        throw new ParserSyncException(ctx, "unknown int literal");
    }

    @Override
    public Typed visitFloatLiteralDouble(final NorseParser.FloatLiteralDoubleContext ctx) {
        if (ctx.FLOAT_LITERAL_DOUBLE() != null) {
            return new Typed.TypedLiteral(TypeCode.DOUBLE, Double.parseDouble(ctx.FLOAT_LITERAL_DOUBLE().getSymbol().getText()));
        } else if (ctx.HEX_FLOAT_LITERAL_DOUBLE() != null) {
            throw new UnsupportedOperationException("hex float literal not supported yet");
        }
        throw new ParserSyncException(ctx, "unknown float literal");
    }

    @Override
    public Typed visitFloatLiteral(final NorseParser.FloatLiteralContext ctx) {
        if (ctx.FLOAT_LITERAL() != null) {
            return new Typed.TypedLiteral(TypeCode.FLOAT, Float.parseFloat(ctx.FLOAT_LITERAL().getSymbol().getText()));
        } else if (ctx.HEX_FLOAT_LITERAL() != null) {
            throw new UnsupportedOperationException("hex float literal not supported yet");
        }
        throw new ParserSyncException(ctx, "unknown float literal");
    }

    @Override
    public Typed visitErrorNode(final ErrorNode node) {
        throw new RuntimeException("unable to parse statement");
    }

    private ParserWalker withScopes(@Nonnull Scopes scopes) {
        return new ParserWalker(new ParserContext(scopes, parserContext.getRecordMetaData(), parserContext.getRecordStoreState()));
    }

    private static <T> T unboxLiteral(@Nonnull Typed t, @Nonnull Class<? extends T> tClass) {
        if (t instanceof Typed.TypedLiteral && tClass.isAssignableFrom(t.getResultType().getJavaClass())) {
            return tClass.cast(((Typed.TypedLiteral)t).getValue());
        }
        throw new IllegalStateException("literal of unexpected type");
    }

    /**
     * Exception class indicating that a parser rule seems to be visited with stale logic.
     */
    public static class ParserSyncException extends RuntimeException {
        private static final long serialVersionUID = -4457853268134025882L;
        @Nonnull
        private final transient ParserRuleContext parserRuleContext;

        public ParserSyncException(@Nonnull final ParserRuleContext parserRuleContext, final String message) {
            super(message);
            this.parserRuleContext = parserRuleContext;
        }

        @Override
        public String getMessage() {
            return parserRuleContext + " " + super.getMessage();
        }
    }
}
