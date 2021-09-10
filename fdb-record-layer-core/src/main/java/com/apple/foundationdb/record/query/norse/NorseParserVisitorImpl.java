/*
 * NorseParserVisitorImpl.java
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

import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Type.TypeCode;
import com.apple.foundationdb.record.query.predicates.Typed;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.predicates.Type.primitiveType;

public class NorseParserVisitorImpl extends NorseParserBaseVisitor<Typed> {

    private final ParserContext parserContext;

    public NorseParserVisitorImpl() {
        this.parserContext = new ParserContext();
    }

    @Override
    public Typed visitPipeMethodCall(final NorseParser.PipeMethodCallContext ctx) {
        final NorseParser.PipeContext pipeContext = Objects.requireNonNull(ctx.pipe());
        final ImmutableList.Builder<Typed> argumentsBuilder = ImmutableList.builder();
        argumentsBuilder.add(pipeContext.accept(this));
        final NorseParser.MethodCallContext methodCallContext = Objects.requireNonNull(ctx.methodCall());
        final String functionName = Objects.requireNonNull(methodCallContext.IDENTIFIER()).getText();
        argumentsBuilder.addAll(callArguments(methodCallContext));
        final List<Typed> arguments = argumentsBuilder.build();
        final Optional<BuiltInFunction<? extends Typed>> functionOptional = FunctionCatalog.resolveFunction(functionName, Type.fromTyped(arguments));
        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve function"));
    }

    @Override
    public Typed visitExpressionFunctionCall(final NorseParser.ExpressionFunctionCallContext ctx) {
        Objects.requireNonNull(ctx.functionCall());
        final NorseParser.MethodCallContext methodCallContext = Objects.requireNonNull(ctx.functionCall().methodCall());
        final String functionName = Objects.requireNonNull(methodCallContext.IDENTIFIER()).getText();
        final List<Typed> arguments = callArguments(methodCallContext);
        final Optional<BuiltInFunction<? extends Typed>> functionOptional = FunctionCatalog.resolveFunction(functionName, Type.fromTyped(arguments));
        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve function"));
    }

    private List<Typed> callArguments(final NorseParser.MethodCallContext ctx) {
        if (ctx.expressionList() != null) {
            final NorseParser.ExpressionListContext expressionListContext = ctx.expressionList();

            return expressionListContext
                    .expression()
                    .stream()
                    .map(argumentExpression -> argumentExpression.accept(this))
                    .collect(ImmutableList.toImmutableList());
        }
        return ImmutableList.of();
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
            functionOptional = FunctionCatalog.resolveFunction("lt", Type.fromTyped(arguments));
        } else if (ctx.LE() != null) {
            functionOptional = FunctionCatalog.resolveFunction("lte", Type.fromTyped(arguments));
        } else if (ctx.GT() != null) {
            functionOptional = FunctionCatalog.resolveFunction("gt", Type.fromTyped(arguments));
        } else if (ctx.GE() != null) {
            functionOptional = FunctionCatalog.resolveFunction("gte", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalArgumentException("unable to resolve comparators"));
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
