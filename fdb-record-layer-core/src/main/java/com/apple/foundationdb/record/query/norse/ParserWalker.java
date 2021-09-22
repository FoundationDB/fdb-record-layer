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
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.predicates.Atom;
import com.apple.foundationdb.record.query.predicates.BooleanValue;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.Lambda;
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Type.TypeCode;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.apple.foundationdb.record.query.predicates.Type.primitiveType;

@SuppressWarnings("UnstableApiUsage")
public class ParserWalker extends NorseParserBaseVisitor<Atom> {

    private final ParserContext parserContext;

    public ParserWalker(@Nonnull RecordMetaData recordMetaData, @Nonnull RecordStoreState recordStoreState) {
        this(new ParserContext(new Scopes().push(ImmutableSet.of(), ImmutableMap.of()), recordMetaData, recordStoreState));
    }

    public ParserWalker(@Nonnull final ParserContext parserContext) {
        this.parserContext = parserContext;
    }

    public ParserContext getParserContext() {
        return parserContext;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public Atom visitPipeMethodCall(final NorseParser.PipeMethodCallContext ctx) {
        final NorseParser.PipeContext pipeContext = Objects.requireNonNull(ctx.pipe());
        final NorseParser.MethodCallContext methodCallContext = Objects.requireNonNull(ctx.methodCall());
        final String functionName = Objects.requireNonNull(methodCallContext.IDENTIFIER()).getText();
        final NorseParser.ArgumentsOrTupleContext argumentsOrTupleContext = Objects.requireNonNull(methodCallContext.argumentsOrTuple());

        Optional<BuiltInFunction<? extends Atom>> functionOptional = Optional.empty();
        Optional<List<Atom>> argumentsOptional = Optional.empty();

        final List<? extends ParserRuleContext> ambiguousArgumentContexts =
                resolveAmbiguousArgumentOrTupleContexts(argumentsOrTupleContext);

        // first try to resolve the method call
        // s.foo(a, b, c)
        // where (a, b, c) is a tuple and foo is a method of signature foo:(S, TUPLE) -> something
        if (!ambiguousArgumentContexts.isEmpty()) {
            functionOptional = FunctionCatalog.resolve(functionName, 2);
            if (functionOptional.isPresent()) {
                final Type type1 = functionOptional.get().resolveParameterType(1);
                if (type1 instanceof Type.Function &&
                        Objects.requireNonNull(((Type.Function)type1).getResultType()).getTypeCode() == TypeCode.STREAM) {
                    final BuiltInFunction<? extends Atom> function = functionOptional.get();
                    final List<Type> resolvedParameterTypes = function.resolveParameterTypes(2);

                    argumentsOptional =
                            Optional.of(ImmutableList.of(callArgument(pipeContext, resolvedParameterTypes.get(0)),
                                    lambdaBodyWithPossibleTuple(ImmutableList.of(), ambiguousArgumentContexts)));
                }
            }
        }

        // now try to resolve the method call
        // s.foo(a, b, c)
        // where (a, b, c) is a list of arguments foo is a method of signature foo:(S, A, B, C) -> something
        if (!functionOptional.isPresent() || !argumentsOptional.isPresent()) {
            final List<? extends ParserRuleContext> argumentContexts = resolveArgumentOrTupleContexts(argumentsOrTupleContext);

            final int numberOfArguments = argumentContexts.size() + 1;
            functionOptional = FunctionCatalog.resolve(functionName, numberOfArguments);
            SemanticException.check(functionOptional.isPresent(), "unable to resolve function " + functionName + "in catalog");

            final List<Type> resolvedParameterTypes = functionOptional.get().resolveParameterTypes(numberOfArguments);
            argumentsOptional =
                    Optional.of(ImmutableList.<Atom>builder()
                            .add(callArgument(pipeContext, resolvedParameterTypes.get(0)))
                            .addAll(Streams.zip(argumentContexts.stream(),
                                    resolvedParameterTypes.subList(1, resolvedParameterTypes.size()).stream(), this::callArgument).collect(ImmutableList.toImmutableList()))
                            .build());
        }

        final List<Atom> arguments = argumentsOptional.orElseThrow(() -> new SemanticException("unable to compile arguments for call to " + functionName));
        return functionOptional
                .flatMap(builtInFunction -> builtInFunction.validateCall(Type.fromTyped(arguments)))
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new SemanticException("unable to compile in function " + functionName));
    }

    @Override
    public Atom visitExpressionField(final NorseParser.ExpressionFieldContext ctx) {
        final NorseParser.ExpressionContext expressionContext = Objects.requireNonNull(ctx.expression());
        final TerminalNode identifier = ctx.IDENTIFIER();

        final Atom atom = expressionContext.accept(this);
        SemanticException.check(atom.getResultType().getTypeCode() == TypeCode.RECORD,
                "context to a field accessor has to be of type record");

        Verify.verify(atom instanceof Value);
        Verify.verify(atom.getResultType() instanceof Type.Record);
        final Type.Record recordType = (Type.Record)atom.getResultType();
        final String fieldName = identifier.getText();
        final Map<String, Type> fieldTypeMap = Objects.requireNonNull(recordType.getFieldTypeMap());
        Preconditions.checkArgument(fieldTypeMap.containsKey(fieldName), "attempting to query non existing field");
        final Type fieldType = fieldTypeMap.get(fieldName);

        if (atom instanceof FieldValue) {
            ImmutableList.Builder<String> fieldPathBuilder = ImmutableList.builder();
            fieldPathBuilder.addAll(((FieldValue)atom).getFieldPath());
            fieldPathBuilder.add(fieldName);
            return new FieldValue(((FieldValue)atom).getChild(), fieldPathBuilder.build(), fieldType);
        } else if (atom instanceof QuantifiedColumnValue) {
            return new FieldValue((QuantifiedColumnValue)atom, ImmutableList.of(fieldName), fieldType);
        }

        // TODO
        throw new UnsupportedOperationException("unable to support arbitrary context expressions");
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public Atom visitExpressionFunctionCall(final NorseParser.ExpressionFunctionCallContext ctx) {
        Objects.requireNonNull(ctx.functionCall());
        final NorseParser.MethodCallContext methodCallContext = Objects.requireNonNull(ctx.functionCall().methodCall());
        final String functionName = Objects.requireNonNull(methodCallContext.IDENTIFIER()).getText();
        final NorseParser.ArgumentsOrTupleContext argumentsOrTupleContext = Objects.requireNonNull(methodCallContext.argumentsOrTuple());

        Optional<BuiltInFunction<? extends Atom>> functionOptional = Optional.empty();
        Optional<List<Atom>> argumentsOptional = Optional.empty();

        final List<? extends ParserRuleContext> ambiguousArgumentContexts =
                resolveAmbiguousArgumentOrTupleContexts(argumentsOrTupleContext);

        // first try to resolve the function call
        // foo(a, b, c)
        // where (a, b, c) is a tuple and foo is a method of signature foo:(TUPLE) -> something
        if (!ambiguousArgumentContexts.isEmpty()) {
            functionOptional = FunctionCatalog.resolve(functionName, 1);
            if (functionOptional.isPresent()) {
                final Type type0 = functionOptional.get().resolveParameterType(0);
                if (type0 instanceof Type.Function &&
                        Objects.requireNonNull(((Type.Function)type0).getResultType()).getTypeCode() == TypeCode.TUPLE) {
                    argumentsOptional =
                            Optional.of(ImmutableList.of(lambdaBodyWithPossibleTuple(ImmutableList.of(), ambiguousArgumentContexts)));
                }
            }
        }

        // now try to resolve the method call
        // foo(a, b, c)
        // where (a, b, c) is a list of arguments foo is a method of signature foo:(A, B, C) -> something
        if (!functionOptional.isPresent() || !argumentsOptional.isPresent()) {
            final List<? extends ParserRuleContext> argumentContexts = resolveArgumentOrTupleContexts(argumentsOrTupleContext);
            functionOptional = FunctionCatalog.resolve(functionName, argumentContexts.size());
            SemanticException.check(functionOptional.isPresent(), "unable to resolve function " + functionName + "in catalog");

            final List<Type> resolvedParameterTypes = functionOptional.get().resolveParameterTypes(argumentContexts.size());
            argumentsOptional = Optional.of(
                    Streams.zip(argumentContexts.stream(), resolvedParameterTypes.stream(), this::callArgument)
                            .collect(ImmutableList.toImmutableList()));

        }

        final List<Atom> arguments = argumentsOptional.orElseThrow(() -> new SemanticException("unable to compile arguments for call to " + functionName));
        return functionOptional
                .flatMap(builtInFunction -> builtInFunction.validateCall(Type.fromTyped(arguments)))
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new SemanticException("unable to compile in function " + functionName));
    }

    private List<? extends ParserRuleContext> resolveAmbiguousArgumentOrTupleContexts(@Nonnull final NorseParser.ArgumentsOrTupleContext ctx) {
        final List<? extends ParserRuleContext> ambiguousArgumentContexts;
        if (ctx instanceof NorseParser.ArgumentsOrTuplePipesContext) {
            ambiguousArgumentContexts = ((NorseParser.ArgumentsOrTuplePipesContext)ctx).pipe();
        } else if (ctx instanceof NorseParser.ArgumentsOrTupleExpressionContext) {
            ambiguousArgumentContexts = ImmutableList.of();
        } else {
            throw new StaleRuleException(ctx, "unknown rule");
        }

        return ambiguousArgumentContexts;
    }

    @Nonnull
    private List<? extends ParserRuleContext> resolveArgumentOrTupleContexts(@Nonnull final NorseParser.ArgumentsOrTupleContext ctx) {
        final List<? extends ParserRuleContext> argumentContexts;
        if (ctx instanceof NorseParser.ArgumentsOrTuplePipesContext) {
            argumentContexts = ((NorseParser.ArgumentsOrTuplePipesContext)ctx).pipe();
        } else if (ctx instanceof NorseParser.ArgumentsOrTupleExpressionContext) {
            argumentContexts = ImmutableList.of(((NorseParser.ArgumentsOrTupleExpressionContext)ctx).expression());
        } else {
            throw new StaleRuleException(ctx, "unknown rule");
        }

        return argumentContexts;
    }

    @Nonnull
    private Lambda lambdaBodyWithPossibleTuple(@Nonnull final List<Optional<String>> declaredParameterNames,
                                               @Nonnull final List<? extends ParserRuleContext> tupleElementContexts) {
        Verify.verify(!tupleElementContexts.isEmpty());
        return lambdaBody(declaredParameterNames,
                parserWalker -> {
                    final ParserContext currentContext = parserWalker.getParserContext();
                    final GraphExpansion.Builder graphExpansionBuilder = currentContext.getCurrentScope().getGraphExpansionBuilder();
                    if (tupleElementContexts.size() > 1) {
                        final ImmutableList<Atom> tupleElements = tupleElementContexts
                                .stream()
                                .map(tupleElementContext -> tupleElementContext.accept(parserWalker))
                                .collect(ImmutableList.toImmutableList());

                        graphExpansionBuilder.addAllAtoms(tupleElements).build();
                    } else {
                        final Atom accept = tupleElementContexts.get(0).accept(parserWalker);
                        Objects.requireNonNull(accept);
                        graphExpansionBuilder.addAtom(accept);
                    }
                });
    }

    @Nonnull
    private Atom callArgument(@Nonnull final ParserRuleContext expressionContext, @Nonnull final Type declaredParameterType) {
        if (declaredParameterType.getTypeCode() == TypeCode.FUNCTION &&
                !(tunnel(expressionContext, currentContext -> currentContext instanceof NorseParser.ExpressionLambdaContext).isPresent())) {
            return lambdaBody(ImmutableList.of(), parserWalker -> {
                final ParserContext currentContext = parserWalker.getParserContext();
                final GraphExpansion.Builder graphExpansionBuilder = currentContext.getCurrentScope().getGraphExpansionBuilder();
                final Atom atom = expressionContext.accept(parserWalker);
                graphExpansionBuilder.addAtom(atom);
            });
        } else {
            return expressionContext.accept(this);
        }
    }

    @Nonnull
    @Override
    public Atom visitExpressionUnaryBang(final NorseParser.ExpressionUnaryBangContext ctx) {
        // visit the children expressions
        final ImmutableList<Atom> arguments =
                ImmutableList.of(ctx.expression().accept(this));

        final Optional<BuiltInFunction<? extends Atom>> functionOptional;
        if (ctx.BANG() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("not", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalStateException("unable to encapsulate not()"));
    }

    @Override
    public Atom visitExpressionInequality(final NorseParser.ExpressionInequalityContext ctx) {
        // visit the children expressions
        final ImmutableList<Atom> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Atom>> functionOptional;
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
                .orElseThrow(() -> new IllegalStateException("unable to encapsulate comparator"));
    }

    @Override
    public Atom visitExpressionEqualityNonEquality(final NorseParser.ExpressionEqualityNonEqualityContext ctx) {
        // visit the children expressions
        final ImmutableList<Atom> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Atom>> functionOptional;
        if (ctx.EQUAL() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("equals", Type.fromTyped(arguments));
        } else if (ctx.NOTEQUAL() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("notEquals", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalStateException("unable to resolve comparators"));
    }

    @Override
    public Atom visitExpressionLogicalAnd(final NorseParser.ExpressionLogicalAndContext ctx) {
        // visit the children expressions
        final ImmutableList<Atom> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Atom>> functionOptional;
        if (ctx.AND() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("and", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalStateException("unable to resolve and()"));
    }

    @Override
    public Atom visitExpressionLogicalOr(final NorseParser.ExpressionLogicalOrContext ctx) {
        // visit the children expressions
        final ImmutableList<Atom> arguments =
                ctx.expression()
                        .stream()
                        .map(expression -> expression.accept(this))
                        .collect(ImmutableList.toImmutableList());

        final Optional<BuiltInFunction<? extends Atom>> functionOptional;
        if (ctx.OR() != null) {
            functionOptional = FunctionCatalog.resolveAndValidate("or", Type.fromTyped(arguments));
        } else {
            functionOptional = Optional.empty();
        }

        return functionOptional
                .map(builtInFunction -> builtInFunction.encapsulate(parserContext, arguments))
                .orElseThrow(() -> new IllegalStateException("unable to resolve or()"));
    }

    @Override
    public Atom visitExpressionLambda(final NorseParser.ExpressionLambdaContext ctx) {
        final NorseParser.LambdaContext lambdaContext = Objects.requireNonNull(ctx.lambda());
        final NorseParser.ArgumentsOrTupleContext argumentsOrTupleContext = Objects.requireNonNull(lambdaContext.argumentsOrTuple());

        final NorseParser.ExtractorContext extractorsContext = Objects.requireNonNull(lambdaContext.extractor());
        final List<Optional<String>> declaredParameterNameOptionals =
                declaredParameterNameOptionals(extractorsContext);

        return lambdaBodyWithPossibleTuple(declaredParameterNameOptionals, resolveArgumentOrTupleContexts(argumentsOrTupleContext));
    }

    @Nonnull
    private ImmutableList<Optional<String>> declaredParameterNameOptionals(final NorseParser.ExtractorContext extractorsContext) {
        return Objects.requireNonNull(extractorsContext.bindingIdentifier())
                .stream()
                .map(bindingIdentifier -> {
                    if (bindingIdentifier.IDENTIFIER() != null) {
                        return Optional.of(bindingIdentifier.IDENTIFIER().getText());
                    }
                    return Optional.<String>empty();
                })
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private Lambda lambdaBody(@Nonnull final List<Optional<String>> parameterNames,
                              @Nonnull final Consumer<ParserWalker> walkerConsumer) {
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
            walkerConsumer.accept(nestedWalker);

            Verify.verify(nestedWalker.getParserContext().getCurrentScope() == callingScopes.getCurrentScope());

            return Objects.requireNonNull(callingScopes.getCurrentScope()).getGraphExpansionBuilder().build();
        });
    }

    @Override
    public Atom visitComprehensionWithBindings(final NorseParser.ComprehensionWithBindingsContext ctx) {
        final List<NorseParser.ComprehensionBindingContext> comprehensionBindingContexts = Objects.requireNonNull(ctx.comprehensionBindings()).comprehensionBinding();

        final ImmutableList.Builder<Quantifier> quantifiersBuilder = ImmutableList.builder();
        final ImmutableList.Builder<QueryPredicate> predicatesBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Value> boundIdentifierToValueMapBuilder = ImmutableMap.builder();

        for (final NorseParser.ComprehensionBindingContext comprehensionBindingContext : comprehensionBindingContexts) {
            final ImmutableMap<String, Value> boundIdentifierToValueMap = boundIdentifierToValueMapBuilder.build();
            final List<Optional<String>> boundVariables =
                    boundVariables(boundIdentifierToValueMap);
            final List<? extends Value> arguments =
                    argementsFromBoundVariables(boundVariables, boundIdentifierToValueMap);

            if (comprehensionBindingContext instanceof NorseParser.ComprehensionBindingIterationContext) {
                final NorseParser.ComprehensionBindingIterationContext bindingContext = (NorseParser.ComprehensionBindingIterationContext)comprehensionBindingContext;
                final NorseParser.ExtractorContext extractorContext = bindingContext.extractor();
                final List<? extends ParserRuleContext> parserRuleContexts = ImmutableList.of(bindingContext.pipe());
                final Lambda lambda = lambdaBodyWithPossibleTuple(boundVariables, parserRuleContexts);
                final GraphExpansion graphExpansion = lambda.unifyBody(arguments);
                quantifiersBuilder.addAll(graphExpansion.getQuantifiers());

                final List<Atom> results = graphExpansion.getResults();
                final Atom resultAtom = Iterables.getOnlyElement(results);

                final RelationalExpression result;
                if (resultAtom.getResultType().getTypeCode() == TypeCode.STREAM) {
                    result = Iterables.getOnlyElement(graphExpansion.getResultsAs(RelationalExpression.class));
                } else if (Iterables.getOnlyElement(results) instanceof Value) {
                    result = new ExplodeExpression(Iterables.getOnlyElement(graphExpansion.getResultsAs(Value.class)));
                } else {
                    throw new IllegalStateException("shouldn't be in this state");
                }
                final Quantifier.ForEach forEach = Quantifier.forEach(GroupExpressionRef.of(result));
                quantifiersBuilder.add(forEach);
                final List<? extends QuantifiedColumnValue> flowedValues = forEach.getFlowedValues();

                Verify.verify(!flowedValues.isEmpty());
                final ImmutableList<Optional<String>> declaredParameterNameOptionals = declaredParameterNameOptionals(extractorContext);
                Preconditions.checkArgument(declaredParameterNameOptionals.size() == flowedValues.size());

                Streams.zip(declaredParameterNameOptionals.stream(), flowedValues.stream(), Pair::of)
                        .filter(pair -> pair.getKey().isPresent()) // skip the columns that are not needed
                        .forEach(pair -> {
                            final String declaredParameterName = pair.getKey().get();
                            SemanticException.check(!boundIdentifierToValueMap.containsKey(declaredParameterName), "duplicate binding for identifier " + declaredParameterName);
                            boundIdentifierToValueMapBuilder.put(declaredParameterName, pair.getValue());
                        });
            } else if (comprehensionBindingContext instanceof NorseParser.ComprehensionBindingAssignContext) {
                final NorseParser.ComprehensionBindingAssignContext bindingContext = (NorseParser.ComprehensionBindingAssignContext)comprehensionBindingContext;
                final Lambda lambda = lambdaBodyWithPossibleTuple(boundVariables, ImmutableList.of(bindingContext.pipe()));
                final GraphExpansion graphExpansion = lambda.unifyBody(arguments);
                quantifiersBuilder.addAll(graphExpansion.getQuantifiers());
                final RelationalExpression result = Iterables.getOnlyElement(graphExpansion.getResultsAs(RelationalExpression.class));
                final Quantifier.ForEach forEach = Quantifier.forEach(GroupExpressionRef.of(result));
                quantifiersBuilder.add(forEach);

                // TODO we need to build a COLLECT() here but there is no such expression yet
                final List<? extends QuantifiedColumnValue> flowedValues = forEach.getFlowedValues();
                Verify.verify(flowedValues.size() == 1);
                final String declaredParameterName = Objects.requireNonNull(bindingContext.IDENTIFIER()).getText();
                Preconditions.checkArgument(!boundIdentifierToValueMap.containsKey(declaredParameterName), "duplicate binding for identifier " + declaredParameterName);
                boundIdentifierToValueMapBuilder.put(declaredParameterName, flowedValues.get(0));
            } else if (comprehensionBindingContext instanceof NorseParser.ComprehensionBindingIfContext) {
                final NorseParser.ComprehensionBindingIfContext bindingContext = (NorseParser.ComprehensionBindingIfContext)comprehensionBindingContext;
                final Lambda lambda = lambdaBodyWithPossibleTuple(boundVariables, ImmutableList.of(bindingContext.pipe()));
                final GraphExpansion graphExpansion = lambda.unifyBody(arguments);
                quantifiersBuilder.addAll(graphExpansion.getQuantifiers());
                final Value value = Iterables.getOnlyElement(graphExpansion.getResultsAs(Value.class));
                Preconditions.checkArgument(value.getResultType().getTypeCode() == TypeCode.BOOLEAN);
                Verify.verify(value instanceof BooleanValue);
                // TODO hack we just grab one alias from the set of correlated aliases
                predicatesBuilder.add(((BooleanValue)value).toQueryPredicate(Iterables.getOnlyElement(value.getCorrelatedTo())).orElseThrow(() -> new IllegalStateException("unable to translate into predicate")));
            } else {
                throw new StaleRuleException(comprehensionBindingContext, "unknown binding rule for comprehension");
            }
        }

        final ImmutableMap<String, Value> boundIdentifierToValueMap = boundIdentifierToValueMapBuilder.build();
        final List<Optional<String>> boundVariables =
                boundVariables(boundIdentifierToValueMap);
        final List<? extends Value> arguments =
                argementsFromBoundVariables(boundVariables, boundIdentifierToValueMap);

        // process result
        final NorseParser.ArgumentsOrTupleContext argumentsOrTupleContext = Objects.requireNonNull(ctx.argumentsOrTuple());
        final List<? extends ParserRuleContext> parserRuleContexts = resolveArgumentOrTupleContexts(argumentsOrTupleContext);
        final Lambda lambda = lambdaBodyWithPossibleTuple(boundVariables, parserRuleContexts);
        final GraphExpansion graphExpansion = lambda.unifyBody(arguments);
        Preconditions.checkArgument(graphExpansion.getResults().stream().noneMatch(typed -> typed.getResultType().getTypeCode() == TypeCode.STREAM));
        final List<? extends Value> resultValues = graphExpansion.getResultsAs(Value.class);
        quantifiersBuilder.addAll(graphExpansion.getQuantifiers());
        final ImmutableList<Quantifier> quantifiers = quantifiersBuilder.build();
        final ImmutableList<QueryPredicate> predicates = predicatesBuilder.build();
        // optimization if there is only one quantifier and there are only trivial result values
        if (quantifiers.size() == 1 &&
                predicates.isEmpty() &&
                resultValues.stream().allMatch(resultValue -> resultValue instanceof QuantifiedColumnValue)) {
            return Iterables.getOnlyElement(Iterables.getOnlyElement(quantifiers).getRangesOver().getMembers());
        }
        return new SelectExpression(resultValues, quantifiers, predicates);
    }

    @Override
    public Atom visitComprehensionSimple(final NorseParser.ComprehensionSimpleContext ctx) {
        final ImmutableList.Builder<Quantifier> quantifiersBuilder = ImmutableList.builder();

        final List<? extends ParserRuleContext> parserRuleContexts = ImmutableList.of(ctx.pipe());
        final Lambda lambda = lambdaBodyWithPossibleTuple(ImmutableList.of(), parserRuleContexts);
        final GraphExpansion graphExpansion = lambda.unifyBody(ImmutableList.of());
        quantifiersBuilder.addAll(graphExpansion.getQuantifiers());

        final List<Atom> results = graphExpansion.getResults();
        final Atom resultAtom = Iterables.getOnlyElement(results);

        final RelationalExpression result;
        if (resultAtom.getResultType().getTypeCode() == TypeCode.STREAM) {
            result = Iterables.getOnlyElement(graphExpansion.getResultsAs(RelationalExpression.class));
        } else if (Iterables.getOnlyElement(results) instanceof Value) {
            result = new ExplodeExpression(Iterables.getOnlyElement(graphExpansion.getResultsAs(Value.class)));
        } else {
            throw new IllegalStateException("shouldn't be in this state");
        }

        final Quantifier.ForEach forEach = Quantifier.forEach(GroupExpressionRef.of(result));
        quantifiersBuilder.add(forEach);
        final List<? extends QuantifiedColumnValue> flowedValues = forEach.getFlowedValues();

        Verify.verify(!flowedValues.isEmpty());

        // process result
        final ImmutableList<Quantifier> quantifiers = quantifiersBuilder.build();
        // optimization if there is only one quantifier and there are only trivial result values
        if (quantifiers.size() == 1) {
            return Iterables.getOnlyElement(Iterables.getOnlyElement(quantifiers).getRangesOver().getMembers());
        } else {
            return new SelectExpression(flowedValues, quantifiers, ImmutableList.of());
        }
    }

    @Nonnull
    private ImmutableList<Optional<String>> boundVariables(@Nonnull final ImmutableMap<String, Value> boundIdentifierToValueMap) {
        return boundIdentifierToValueMap.keySet()
                .stream()
                .map(Optional::of)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private ImmutableList<Value> argementsFromBoundVariables(@Nonnull final List<Optional<String>> boundVariables, @Nonnull final ImmutableMap<String, Value> boundIdentifierToValueMap) {
        return boundVariables.stream()
                .map(boundVariable -> Objects.requireNonNull(
                        boundIdentifierToValueMap.get(boundVariable.orElseThrow(() -> new IllegalStateException("impossible situation")))))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Atom visitPrimaryExpressionNestedPipe(final NorseParser.PrimaryExpressionNestedPipeContext ctx) {
        return ctx.pipe().accept(this);
    }

    @Override
    public Atom visitPrimaryExpressionFromRecordConstructor(final NorseParser.PrimaryExpressionFromRecordConstructorContext ctx) {
        throw new UnsupportedOperationException("unable to construct record");
    }

    @Override
    public Atom visitPrimaryExpressionFromLiteral(final NorseParser.PrimaryExpressionFromLiteralContext ctx) {
        return super.visitPrimaryExpressionFromLiteral(ctx);
    }

    @Override
    public Atom visitPrimaryExpressionFromUnderbar(final NorseParser.PrimaryExpressionFromUnderbarContext ctx) {
        return parserContext.resolveIdentifier(ctx.UNDERBAR().getSymbol().getText());
    }

    @Override
    public Atom visitPrimaryExpressionFromIdentifier(final NorseParser.PrimaryExpressionFromIdentifierContext ctx) {
        return parserContext.resolveIdentifier(ctx.IDENTIFIER().getSymbol().getText());
    }

    @Override
    public Atom visitLiteral(final NorseParser.LiteralContext ctx) {
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

        throw new StaleRuleException(ctx, "unknown rule");
    }

    @Override
    public Atom visitIntegerLiteralLong(final NorseParser.IntegerLiteralLongContext ctx) {
        if (ctx.DECIMAL_LITERAL_LONG() != null) {
            final String text = ctx.DECIMAL_LITERAL_LONG().getSymbol().getText();
            // strip the "lL" bit at the end
            return new Atom.AtomLiteral(TypeCode.LONG, Long.parseLong(text.substring(0, text.length() - 1)));
        } else if (ctx.BINARY_LITERAL_LONG() != null ||
                   ctx.HEX_LITERAL_LONG() != null  ||
                   ctx.OCT_LITERAL_LONG() != null) {
            throw new UnsupportedOperationException("(binary | hex | oct) int literal not supported yet");
        }
        throw new StaleRuleException(ctx, "unknown int literal");
    }

    @Override
    public Atom visitIntegerLiteral(final NorseParser.IntegerLiteralContext ctx) {
        if (ctx.DECIMAL_LITERAL() != null) {
            return new Atom.AtomLiteral(TypeCode.INT, Integer.parseInt(ctx.DECIMAL_LITERAL().getSymbol().getText()));
        } else if (ctx.BINARY_LITERAL() != null ||
                   ctx.HEX_LITERAL() != null  ||
                   ctx.OCT_LITERAL() != null) {
            throw new UnsupportedOperationException("(binary | hex | oct) int literal not supported yet");
        }
        throw new StaleRuleException(ctx, "unknown int literal");
    }

    @Override
    public Atom visitFloatLiteralDouble(final NorseParser.FloatLiteralDoubleContext ctx) {
        if (ctx.FLOAT_LITERAL_DOUBLE() != null) {
            return new Atom.AtomLiteral(TypeCode.DOUBLE, Double.parseDouble(ctx.FLOAT_LITERAL_DOUBLE().getSymbol().getText()));
        } else if (ctx.HEX_FLOAT_LITERAL_DOUBLE() != null) {
            throw new UnsupportedOperationException("hex float literal not supported yet");
        }
        throw new StaleRuleException(ctx, "unknown float literal");
    }

    @Override
    public Atom visitFloatLiteral(final NorseParser.FloatLiteralContext ctx) {
        if (ctx.FLOAT_LITERAL() != null) {
            return new Atom.AtomLiteral(TypeCode.FLOAT, Float.parseFloat(ctx.FLOAT_LITERAL().getSymbol().getText()));
        } else if (ctx.HEX_FLOAT_LITERAL() != null) {
            throw new UnsupportedOperationException("hex float literal not supported yet");
        }
        throw new StaleRuleException(ctx, "unknown float literal");
    }

    @Override
    public Atom visitErrorNode(final ErrorNode node) {
        throw new ParseException(node, "unable to parse statement");
    }

    private ParserWalker withScopes(@Nonnull Scopes scopes) {
        return new ParserWalker(new ParserContext(scopes, parserContext.getRecordMetaData(), parserContext.getRecordStoreState()));
    }

    private static <T> T unboxLiteral(@Nonnull Atom t, @Nonnull Class<? extends T> tClass) {
        if (t instanceof Atom.AtomLiteral && tClass.isAssignableFrom(t.getResultType().getJavaClass())) {
            return tClass.cast(((Atom.AtomLiteral)t).getValue());
        }
        throw new IllegalStateException("literal of unexpected type");
    }

    @Nonnull
    private static Optional<ParserRuleContext> tunnel(@Nonnull final ParserRuleContext startContext, @Nonnull Predicate<ParserRuleContext> contextPredicate) {
        ParserRuleContext currentContext = startContext;
        while (!contextPredicate.test(currentContext)) {
            if (currentContext.children == null || currentContext.children.size() > 1) {
                return Optional.empty();
            }

            final ParseTree child0 = currentContext.getChild(0);
            if (child0 == null || child0 instanceof TerminalNode) {
                return Optional.empty();
            }

            if (child0 instanceof ParserRuleContext) {
                currentContext = (ParserRuleContext)child0;
            }
        }
        return Optional.of(currentContext);
    }

    /**
     * Exception class indicating that a parser rule seems to be visited with stale logic.
     */
    public static class StaleRuleException extends RuntimeException {
        private static final long serialVersionUID = -4457853268134025882L;
        @Nonnull
        private final transient ParserRuleContext parserRuleContext;

        public StaleRuleException(@Nonnull final ParserRuleContext parserRuleContext, final String message) {
            super(message);
            this.parserRuleContext = parserRuleContext;
        }

        @Override
        public String getMessage() {
            return parserRuleContext + " " + super.getMessage();
        }
    }

    public static class ParseException extends RuntimeException {
        private static final long serialVersionUID = 7589269615221869588L;
        @Nonnull
        private final transient ErrorNode errorNode;

        public ParseException(@Nonnull final ErrorNode errorNode, final String message) {
            super(message);
            this.errorNode = errorNode;
        }

        @Nonnull
        public ErrorNode getErrorNode() {
            return errorNode;
        }
    }
}
