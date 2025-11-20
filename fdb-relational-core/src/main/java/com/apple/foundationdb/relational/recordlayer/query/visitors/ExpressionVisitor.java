/*
 * ExpressionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.visitors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConditionSelectorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ExistsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.CastValue;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalPlanFragment;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.ParseHelpers;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.StringTrieNode;
import com.apple.foundationdb.relational.recordlayer.query.TautologicalValue;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.protobuf.ZeroCopyByteString;
import org.antlr.v4.runtime.ParserRuleContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This visits expression tree parse nodes and generates a corresponding {@link Expression}.
 */
@API(API.Status.EXPERIMENTAL)
public final class ExpressionVisitor extends DelegatingVisitor<BaseVisitor> {

    private ExpressionVisitor(@Nonnull BaseVisitor baseVisitor) {
        super(baseVisitor);
    }

    @Nonnull
    public static ExpressionVisitor of(@Nonnull BaseVisitor baseVisitor) {
        return new ExpressionVisitor(baseVisitor);
    }

    @Override
    public LogicalOperator visitTableFunction(@Nonnull RelationalParser.TableFunctionContext ctx) {
        final var functionName = visitTableFunctionName(ctx.tableFunctionName());
        return ctx.tableFunctionArgs() == null
                ? getDelegate().resolveTableValuedFunction(functionName, Expressions.empty())
                : getDelegate().resolveTableValuedFunction(functionName, visitTableFunctionArgs(ctx.tableFunctionArgs()));
    }

    @Override
    public Expressions visitTableFunctionArgs(@Nonnull final RelationalParser.TableFunctionArgsContext ctx) {
        if (!ctx.namedFunctionArg().isEmpty()) {
            final var namedArguments = Expressions.of(ctx.namedFunctionArg().stream()
                    .map(this::visitNamedFunctionArg).collect(ImmutableList.toImmutableList()));
            final var duplicateArguments = namedArguments.asList().stream().flatMap(p -> p.getName().stream())
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .entrySet()
                    .stream()
                    .filter(p -> p.getValue() > 1)
                    .collect(ImmutableList.toImmutableList());
            Assert.thatUnchecked(duplicateArguments.isEmpty(), ErrorCode.SYNTAX_ERROR, () ->
                    "argument name(s) used more than once" + duplicateArguments.stream()
                            .map(Object::toString).collect(Collectors.joining(",")));
            return namedArguments;
        } else {
            return Expressions.of(ctx.functionArg().stream().map(this::visitFunctionArg)
                    .collect(ImmutableList.toImmutableList()));
        }
    }

    @Override
    public Expression visitNamedFunctionArg(@Nonnull final RelationalParser.NamedFunctionArgContext ctx) {
        final var name = visitUid(ctx.key);
        final var expression = Assert.castUnchecked(visit(ctx.value), Expression.class);
        return expression.toNamedArgument(name);
    }

    @Nonnull
    @Override
    public Expression visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx) {
        return getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() -> {
            final var continuationExpression = parseChild(ctx);
            SemanticAnalyzer.validateContinuation(continuationExpression);
            return continuationExpression;
        });
    }

    @Nonnull
    @Override
    public Expression visitSelectStarElement(@Nonnull RelationalParser.SelectStarElementContext ignored) {
        return getDelegate().getSemanticAnalyzer().expandStar(Optional.empty(), getDelegate().getLogicalOperators());
    }

    @Nonnull
    @Override
    public Expression visitSelectQualifierStarElement(@Nonnull RelationalParser.SelectQualifierStarElementContext ctx) {
        final var identifier = visitUid(ctx.uid());
        // the semantics of valid correlations are extended to expanding a (correlated) qualified star.
        return getDelegate().getSemanticAnalyzer().expandStar(Optional.of(identifier), getDelegate().getLogicalOperatorsIncludingOuter());
    }

    @Nonnull
    @Override
    public Expression visitFullColumnNameExpressionAtom(@Nonnull RelationalParser.FullColumnNameExpressionAtomContext fullColumnNameExpressionAtomContext) {
        return Assert.castUnchecked(fullColumnNameExpressionAtomContext.fullColumnName().accept(this), Expression.class);
    }

    @Nonnull
    @Override
    public Expressions visitSelectElements(@Nonnull RelationalParser.SelectElementsContext selectElementsContext) {
        return Expressions.of(selectElementsContext.selectElement().stream()
                .map(selectElement -> Assert.castUnchecked(selectElement.accept(this), Expression.class))
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    @Override
    public Expression visitSelectExpressionElement(@Nonnull RelationalParser.SelectExpressionElementContext selectExpressionElementContext) {
        final var expression = Assert.castUnchecked(selectExpressionElementContext.expression().accept(this), Expression.class);
        if (selectExpressionElementContext.AS() != null) {
            final var expressionName = visitUid(selectExpressionElementContext.uid());
            return expression.withName(expressionName);
        }
        return expression;
    }

    @Nonnull
    @Override
    public Expression visitFullColumnName(@Nonnull RelationalParser.FullColumnNameContext fullColumnNameContext) {
        final var id = visitFullId(fullColumnNameContext.fullId());
        return getDelegate().getSemanticAnalyzer().resolveIdentifier(id, getDelegate().getCurrentPlanFragment());
    }

    @Nonnull
    @Override
    public List<OrderByExpression> visitOrderByClause(@Nonnull RelationalParser.OrderByClauseContext orderByClauseContextContext) {
        if (!getDelegate().isTopLevel()) {
            Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "order by is not supported in subquery");
        }
        final var orderByExpressions = orderByClauseContextContext.orderByExpression().stream().map(this::visitOrderByExpression)
                .collect(ImmutableList.toImmutableList());
        getDelegate().getSemanticAnalyzer().validateOrderByColumns(orderByExpressions);
        return orderByExpressions;
    }

    @Nonnull
    @Override
    public OrderByExpression visitOrderByExpression(@Nonnull RelationalParser.OrderByExpressionContext orderByExpressionContext) {
        final var expression = Assert.castUnchecked(orderByExpressionContext.expression().accept(this), Expression.class);
        final var descending = ParseHelpers.isDescending(orderByExpressionContext);
        final var nullsLast = ParseHelpers.isNullsLast(orderByExpressionContext, descending);
        return OrderByExpression.of(expression, descending, nullsLast);
    }

    @Nonnull
    @Override
    public NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> visitInlineTableDefinition(@Nonnull RelationalParser.InlineTableDefinitionContext ctx) {
        final var tableId = visitTableName(ctx.tableName());
        final var columnIdTrie = visitUidListWithNestingsInParens(ctx.uidListWithNestingsInParens());
        int columnCount = Objects.requireNonNull(columnIdTrie.getThis().getChildrenMap()).size();
        final var columnsList = new ArrayList<>(Collections.nCopies(columnCount, (RecordLayerColumn) null));
        for (final var entry : columnIdTrie.getThis().getChildrenMap().entrySet()) {
            final var column = toColumn(entry.getKey(), entry.getValue());
            columnsList.set(column.getIndex(), column);
        }
        final var tableBuilder = RecordLayerTable.newBuilder(false).setName(tableId.getName());
        columnsList.forEach(tableBuilder::addColumn);
        return NonnullPair.of(tableId.getName(), columnIdTrie);
    }

    private static RecordLayerColumn toColumn(@Nonnull FieldValue.ResolvedAccessor field, @Nonnull CompatibleTypeEvolutionPredicate.FieldAccessTrieNode columnIdTrie) {
        final var columnName = field.getName();
        final var builder = RecordLayerColumn.newBuilder().setName(columnName).setIndex(field.getOrdinal());
        if (columnIdTrie.getChildrenMap() == null) {
            return builder.setDataType(DataTypeUtils.toRelationalType(field.getType())).build();
        }
        int columnCount = columnIdTrie.getChildrenMap().size();
        final var fields = new ArrayList<>(Collections.nCopies(columnCount, (DataType.StructType.Field) null));
        for (final var child : columnIdTrie.getChildrenMap().entrySet()) {
            final var column = toColumn(child.getKey(), child.getValue());
            fields.set(column.getIndex(), DataType.StructType.Field.from(column.getName(), column.getDataType(), column.getIndex()));
        }
        builder.setDataType(DataType.StructType.from(columnName, fields, true));
        return builder.build();
    }

    @Nonnull
    @Override
    public Expressions visitGroupByClause(@Nonnull RelationalParser.GroupByClauseContext groupByClauseContext) {
        return Expressions.of(groupByClauseContext.groupByItem().stream().map(this::visitGroupByItem).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    @Override
    public Expression visitGroupByItem(@Nonnull RelationalParser.GroupByItemContext groupByItemContext) {
        Assert.isNullUnchecked(groupByItemContext.order, ErrorCode.UNSUPPORTED_QUERY, "ordering grouping column is not supported");
        final var expression = Assert.castUnchecked(groupByItemContext.expression().accept(this), Expression.class);
        if (groupByItemContext.uid() != null) {
            final var name = visitUid(groupByItemContext.uid());
            return expression.withName(name).asEphemeral();
        }
        return expression;
    }

    @Nonnull
    @Override
    public Expression visitAggregateFunctionCall(@Nonnull RelationalParser.AggregateFunctionCallContext functionCon) {
        return visitAggregateWindowedFunction(functionCon.aggregateWindowedFunction());
    }

    @Nonnull
    @Override
    public Expression visitAggregateWindowedFunction(@Nonnull RelationalParser.AggregateWindowedFunctionContext functionContext) {
        Assert.thatUnchecked(functionContext.aggregator == null || functionContext.aggregator.getText().equals(functionContext.ALL().getText()),
                ErrorCode.UNSUPPORTED_QUERY, () -> String.format(Locale.ROOT, "Unsupported aggregator %s", functionContext.aggregator.getText()));
        final var functionName = functionContext.functionName.getText();
        Optional<Expression> argumentMaybe = Optional.empty();
        if (functionContext.starArg != null) {
            argumentMaybe = Optional.of(Expression.ofUnnamed(RecordConstructorValue.ofColumns(List.of())));
        } else if (functionContext.functionArg() != null) {
            argumentMaybe = Optional.of(visitFunctionArg(functionContext.functionArg()));
        }
        return argumentMaybe.map(expression -> getDelegate().resolveFunction(functionName, expression)).orElseGet(() -> getDelegate().resolveFunction(functionName));
    }

    @Nonnull
    @Override
    public Expression visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx) {
        final var functionName = ctx.scalarFunctionName().getText();
        // special case for user-defined functions where we want to exclude the first argument from
        // being literal-stripped.
        @Nonnull Expressions arguments;
        boolean isUdf = getDelegate().getSemanticAnalyzer().isJavaCallFunction(functionName);
        if (isUdf) {
            final var argumentNodes = ctx.functionArgs().children.stream()
                    .filter(arg -> arg instanceof RelationalParser.FunctionArgContext)
                    .map(RelationalParser.FunctionArgContext.class::cast)
                    .collect(Collectors.toUnmodifiableList());
            Assert.thatUnchecked(!argumentNodes.isEmpty());
            final var classNameExpression = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() -> {
                final var result = visitFunctionArg(argumentNodes.get(0));
                Assert.thatUnchecked(result.getUnderlying() instanceof LiteralValue,
                        ErrorCode.INVALID_ARGUMENT_FOR_FUNCTION, () -> String.format(Locale.ROOT, "attempt to invoke java_call with incorrect UDF '%s'",
                                result.getUnderlying()));
                return result;
            });
            arguments = Expressions.of(Streams.concat(Stream.of(classNameExpression),
                    argumentNodes.stream().skip(1).map(this::visitFunctionArg))
                    .collect(Collectors.toUnmodifiableList()));
        } else {
            arguments = visitFunctionArgs(ctx.functionArgs());
        }
        return getDelegate().resolveFunction(functionName, arguments.asList().toArray(new Expression[0]));
    }

    @Nonnull
    @Override
    public Expression visitUserDefinedScalarFunctionCall(@Nonnull RelationalParser.UserDefinedScalarFunctionCallContext ctx) {
        final var functionName = Identifier.of(getDelegate().normalizeString(ctx.userDefinedScalarFunctionName().getText()));

        // final var functionName = ctx.userDefinedScalarFunctionName().getText();
        Expressions arguments = visitFunctionArgs(ctx.functionArgs());
        return getDelegate().resolveFunction(functionName.getName(), arguments.asList().toArray(new Expression[0]));
    }

    @Nonnull
    @Override
    public Expression visitCaseFunctionCall(@Nonnull RelationalParser.CaseFunctionCallContext ctx) {
        final ImmutableList.Builder<Value> implications = ImmutableList.builder();
        final ImmutableList.Builder<Expression> pickerValues = ImmutableList.builder();
        for (final var caseAlternative : ctx.caseFuncAlternative()) {
            final var condition = visitFunctionArg(caseAlternative.condition);
            Assert.thatUnchecked(condition.getDataType().getCode().equals(DataType.Code.BOOLEAN), ErrorCode.DATATYPE_MISMATCH,
                    "argument of case when must be of boolean type");
            final var consequent = visitFunctionArg(caseAlternative.consequent);
            implications.add(condition.getUnderlying());
            pickerValues.add(consequent);
        }
        if (ctx.ELSE() != null) {
            implications.add(TautologicalValue.getInstance());
            final var defaultConsequent = visitFunctionArg(ctx.functionArg());
            pickerValues.add(defaultConsequent);
        }
        final var arguments = ImmutableList.<Expression>builder();
        arguments.add(Expression.ofUnnamed(new ConditionSelectorValue(implications.build())));
        arguments.addAll(pickerValues.build());
        return getDelegate().resolveFunction("__pick_value", arguments.build().toArray(new Expression[0]));
    }

    @Nonnull
    @Override
    public Expression visitDataTypeFunctionCall(@Nonnull RelationalParser.DataTypeFunctionCallContext ctx) {
        if (ctx.CAST() != null) {
            final var sourceExpression = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
            final var isRepeated = ctx.convertedDataType().ARRAY() != null;
            final var typeInfo = SemanticAnalyzer.ParsedTypeInfo.ofPrimitiveType(ctx.convertedDataType().typeName, false, isRepeated);
            // Cast does not currently support user-defined struct types.
            final var targetDataType = getDelegate().getSemanticAnalyzer().lookupBuiltInType(typeInfo);
            final var targetType = DataTypeUtils.toRecordLayerType(targetDataType);
            final var castValue = CastValue.inject(sourceExpression.getUnderlying(), targetType);
            return Expression.ofUnnamed(targetDataType, castValue);
        }

        Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "CONVERT function is not yet supported");
        return null;
    }

    @Nonnull
    @Override
    public Expression visitFunctionCallExpressionAtom(@Nonnull RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return parseChild(ctx);
    }

    @Nonnull
    @Override
    public Expression visitFunctionArg(@Nonnull RelationalParser.FunctionArgContext functionArgContext) {
        return Assert.castUnchecked(functionArgContext.expression().accept(this), Expression.class);
    }

    @Nonnull
    @Override
    public Expressions visitFunctionArgs(@Nonnull RelationalParser.FunctionArgsContext ctx) {
        return Expressions.of(ctx.functionArg().stream().map(this::visitFunctionArg).collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    @Override
    public Expression visitHavingClause(@Nonnull RelationalParser.HavingClauseContext havingClauseContext) {
        return Assert.castUnchecked(havingClauseContext.expression().accept(this), Expression.class);
    }

    @Nonnull
    @Override
    public Expression visitPreparedStatementParameterAtom(@Nonnull RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return visitPreparedStatementParameter(ctx.preparedStatementParameter());
    }

    @Nonnull
    @Override
    public Expression visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx) {
        final var tokenIndex = ctx.getStart().getTokenIndex();
        final Value value;
        if (ctx.QUESTION() != null) {
            value = getDelegate().getPlanGenerationContext().processUnnamedPreparedParam(tokenIndex);
        } else {
            final String parameterName = ctx.NAMED_PARAMETER().getText().substring(1); // starts with ?, e.g. ?foo
            value = getDelegate().getPlanGenerationContext().processNamedPreparedParam(parameterName, tokenIndex);
        }
        final var type = DataTypeUtils.toRelationalType(value.getResultType());
        return Expression.ofUnnamed(type, value);
    }

    @Nonnull
    @Override
    public Expression visitNotExpression(@Nonnull RelationalParser.NotExpressionContext ctx) {
        final var argument = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.NOT().getText(), argument);
    }

    @Nonnull
    @Override
    public Expression visitLogicalExpression(@Nonnull RelationalParser.LogicalExpressionContext ctx) {
        final var left = Assert.castUnchecked(ctx.expression(0).accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.expression(1).accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.logicalOperator().getText(), left, right);
    }

    @Nonnull
    @Override
    public Expression visitPredicatedExpression(@Nonnull final RelationalParser.PredicatedExpressionContext ctx) {
        final var operand = Assert.castUnchecked(visit(ctx.expressionAtom()), Expression.class);
        final var predicate = ctx.predicate();
        if (predicate == null) {
            return operand;
        }
        if (predicate instanceof RelationalParser.BetweenComparisonPredicateContext) {
            return visitBetweenComparisonPredicate(operand, (RelationalParser.BetweenComparisonPredicateContext)predicate);
        }
        if (predicate instanceof RelationalParser.InPredicateContext) {
            return visitInPredicate(operand, (RelationalParser.InPredicateContext)predicate);
        }
        if (predicate instanceof RelationalParser.LikePredicateContext) {
            return visitLikePredicate(operand, (RelationalParser.LikePredicateContext)predicate);
        }
        if (predicate instanceof RelationalParser.IsExpressionContext) {
            return visitIsExpression(operand, (RelationalParser.IsExpressionContext)predicate);
        }
        Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "unsupported predicate " + ctx.predicate().getText());
        return null;
    }

    @Nonnull
    @Override
    public Expression visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx) {
        // TODO (SQL query with OFFSET clause skipping wrong number of records with splitLongRecords=true in Relational)
        Assert.isNullUnchecked(ctx.offset, "OFFSET clause is not supported");
        // the child must be literal not a ConstantObjectValue because the limit does not contribute anything
        // to the plan, instead, it merely controls the physical execution cursor.
        return getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() -> {
            final var limitExpression = parseChild(ctx);
            SemanticAnalyzer.validateLimit(limitExpression);
            return limitExpression;
        });
    }

    @Nonnull
    @Override
    public Expression visitLimitClauseAtom(@Nonnull RelationalParser.LimitClauseAtomContext ctx) {
        return parseChild(ctx);
    }

    ///// Predicates ///////

    @Nonnull
    @Override
    public Expression visitExistsExpressionAtom(@Nonnull RelationalParser.ExistsExpressionAtomContext ctx) {
        /*
         * (yhatem) this is an interesting visitation, as it requires three interactions:
         * - Firstly, LogicalOperator-visitor calls Expression-visitor to visit this predicate.
         * - Secondly, Expression-visitor calls into LogicalOperator to visit the subquery.
         * - Thirdly, Expression-visitor wraps the subquery in Exists predicate and returns
         *   it to LogicalOperator-visitor.
         */
        final var selectOperator = visitQuery(ctx.query());
        final var asExistential = selectOperator.withQuantifier(Quantifier.existential(selectOperator.getQuantifier().getRangesOver()));
        final var underlyingValue = new ExistsValue(asExistential.getQuantifier().getAlias());
        getDelegate().getCurrentPlanFragment().addOperator(asExistential);
        return Expression.ofUnnamed(underlyingValue);
    }

    @Nonnull
    private Expression visitIsExpression(@Nonnull Expression operand, @Nonnull RelationalParser.IsExpressionContext ctx) {
        if (ctx.NULL_LITERAL() != null) {
            if (ctx.NOT() != null) {
                return getDelegate().resolveFunction("is not null", operand);
            }
            return getDelegate().resolveFunction("is null", operand);
        } else {
            boolean right = ctx.TRUE() != null;
            final Expression nullClause;
            final String combineFunc;
            if (ctx.NOT() != null) {
                //invert the condition, and add an allowance for null as well -- e.g. is not true => (is false or is null)
                right = !right;
                nullClause = getDelegate().resolveFunction("is null", operand);
                combineFunc = "or";
            } else {
                nullClause = getDelegate().resolveFunction("is not null", operand);
                combineFunc = "and";
            }
            final var equals = getDelegate().resolveFunction("=", operand, Expression.ofUnnamed(new LiteralValue<>(right)));
            return getDelegate().resolveFunction(combineFunc, nullClause, equals);
        }
    }

    @Nonnull
    private Expression visitLikePredicate(@Nonnull Expression operand, @Nonnull RelationalParser.LikePredicateContext ctx) {
        final LiteralValue<?> escapeValue;
        if (ctx.escape != null) {
            final var escapeChar = getDelegate().normalizeString(ctx.escape.getText());
            Assert.thatUnchecked(escapeChar.length() == 1);
            escapeValue = new LiteralValue<>(escapeChar);
        } else {
            escapeValue = new LiteralValue<>(null);
        }
        final var pattern = Assert.notNullUnchecked(getDelegate().normalizeString(ctx.pattern.getText()));
        final var patternValueBinding = getDelegate().getPlanGenerationContext().processQueryLiteral(
                Type.primitiveType(Type.TypeCode.STRING), pattern, ctx.pattern.getTokenIndex());
        final var patternFunction = getDelegate().resolveFunction("__pattern_for_like", Expression.ofUnnamed(patternValueBinding),
                Expression.ofUnnamed(escapeValue));
        final var likeFunction = getDelegate().resolveFunction(ctx.LIKE().getText(), operand, patternFunction);
        if (ctx.NOT() != null) {
            return getDelegate().resolveFunction(ctx.NOT().getText(), likeFunction);
        }
        return likeFunction;
    }

    @Nonnull
    private Expression visitInPredicate(@Nonnull Expression operand, @Nonnull RelationalParser.InPredicateContext ctx) {
        Assert.thatUnchecked(ctx.inList().queryExpressionBody() == null, ErrorCode.UNSUPPORTED_QUERY,
                "IN predicate does not support nested SELECT");
        final var right = visitInList(ctx.inList());
        var in = getDelegate().resolveFunction(ctx.IN().getText(), operand, right);
        if (ctx.NOT() != null) {
            in = getDelegate().resolveFunction(ctx.NOT().getText(), in);
        }
        return in;
    }

    @Nonnull
    @Override
    public Expression visitInList(@Nonnull RelationalParser.InListContext ctx) {
        final Expression result;
        if (ctx.preparedStatementParameter() != null) {
            result = visitPreparedStatementParameter(ctx.preparedStatementParameter());
        } else if (ctx.fullColumnName() != null) {
            result = visitFullColumnName(ctx.fullColumnName());
            // Validate that result is of type Array
            Assert.thatUnchecked(result.getDataType().getCode() == DataType.Code.ARRAY, ErrorCode.UNSUPPORTED_QUERY, "IN list with column reference must be of array type, but got: %s", result.getDataType().getCode());
        } else if (getDelegate().getPlanGenerationContext().shouldProcessLiteral() && ParseHelpers.isConstant(ctx.expressions())) {
            getDelegate().getPlanGenerationContext().startArrayLiteral();
            final var inListItems = visitExpressions(ctx.expressions());
            final var tokenIndex = ctx.getStart().getTokenIndex();
            getDelegate().getPlanGenerationContext().finishArrayLiteral(null, null, tokenIndex);
            final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
            semanticAnalyzer.validateInListItems(inListItems);
            final var arrayType = semanticAnalyzer.resolveArrayTypeFromValues(inListItems);
            result = Expression.ofUnnamed(getDelegate().getPlanGenerationContext()
                    .processComplexLiteral(tokenIndex, arrayType));
        } else {
            final var inListItems = visitExpressions(ctx.expressions());
            result = getDelegate().resolveFunction("__internal_array", inListItems.asList().toArray(new Expression[0]));
        }
        return result;
    }

    @Nonnull
    @Override
    public Expression visitWhereExpr(@Nonnull RelationalParser.WhereExprContext ctx) {
        return parseChild(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitExpressions(@Nonnull RelationalParser.ExpressionsContext ctx) {
        return Expressions.of(ctx.expression()
                .stream()
                .map(expression -> Assert.castUnchecked(expression.accept(this), Expression.class))
                .collect(ImmutableList.toImmutableList()));
    }

    @Nonnull
    @Override
    public Expression visitBitExpressionAtom(@Nonnull RelationalParser.BitExpressionAtomContext ctx) {
        final var left = Assert.castUnchecked(ctx.left.accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.right.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.bitOperator().getText(), left, right);
    }

    @Nonnull
    @Override
    public Expression visitBinaryComparisonPredicate(@Nonnull RelationalParser.BinaryComparisonPredicateContext ctx) {
        final var left = Assert.castUnchecked(ctx.left.accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.right.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.comparisonOperator().getText(), left, right);
    }

    @Override
    public Expression visitSubscriptExpression(@Nonnull RelationalParser.SubscriptExpressionContext ctx) {
        final var index = Assert.castUnchecked(ctx.index.accept(this), Expression.class);
        final var base = Assert.castUnchecked(ctx.base.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.LEFT_SQUARE_BRACKET().getText()
                .concat(ctx.RIGHT_SQUARE_BRACKET().getText()), index, base);
    }

    @Nonnull
    private Expression visitBetweenComparisonPredicate(@Nonnull Expression operand,
                                                       @Nonnull RelationalParser.BetweenComparisonPredicateContext ctx) {
        final var left = Assert.castUnchecked(ctx.left.accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.right.accept(this), Expression.class);
        if (ctx.NOT() == null) {
            return getDelegate().resolveFunction("and",
                    getDelegate().resolveFunction("<=", left, operand),
                    getDelegate().resolveFunction("<=", operand, right));
        } else {
            return getDelegate().resolveFunction("or",
                    getDelegate().resolveFunction("<", operand, left),
                    getDelegate().resolveFunction(">", operand, right));
        }
    }

    @Nonnull
    @Override
    public Expression visitMathExpressionAtom(@Nonnull RelationalParser.MathExpressionAtomContext ctx) {
        final var left = Assert.castUnchecked(ctx.left.accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.right.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.mathOperator().getText(), left, right);
    }

    @Nonnull
    @Override
    public Expression visitExpressionWithOptionalName(@Nonnull RelationalParser.ExpressionWithOptionalNameContext ctx) {
        final var expression = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
        if (ctx.AS() != null) {
            final var name = visitUid(ctx.uid());
            return expression.withName(name);
        }
        return expression;
    }

    /////// Literals and Constants //////

    @Nonnull
    @Override
    public Expression visitDecimalLiteral(@Nonnull RelationalParser.DecimalLiteralContext ctx) {
        return resolveDecimal(ctx.getText(), ctx.getStart().getTokenIndex());
    }

    @Nonnull
    @Override
    public Expression visitStringLiteral(@Nonnull RelationalParser.StringLiteralContext ctx) {
        Assert.isNullUnchecked(ctx.STRING_CHARSET_NAME(), ErrorCode.UNSUPPORTED_QUERY, "charset not is supported");
        Assert.isNullUnchecked(ctx.START_NATIONAL_STRING_LITERAL(), ErrorCode.UNSUPPORTED_QUERY, "national string literal is not supported");
        Assert.isNullUnchecked(ctx.COLLATE(), ErrorCode.UNSUPPORTED_QUERY, "collation is not supported");
        final var value = getDelegate().getPlanGenerationContext().processQueryLiteral(Type.primitiveType(Type.TypeCode.STRING),
                getDelegate().normalizeString(ctx.getText()),
                ctx.getStart().getTokenIndex());
        return Expression.ofUnnamed(value);
    }

    @Nonnull
    @Override
    public Expression visitBooleanLiteral(@Nonnull RelationalParser.BooleanLiteralContext ctx) {
        final Value booleanValue;
        if (ctx.FALSE() != null) {
            booleanValue = getDelegate().getPlanGenerationContext().processQueryLiteral(Type.primitiveType(Type.TypeCode.BOOLEAN), Boolean.FALSE,
                    ctx.FALSE().getSymbol().getTokenIndex());
        } else {
            Assert.notNullUnchecked(ctx.TRUE());
            booleanValue = getDelegate().getPlanGenerationContext().processQueryLiteral(Type.primitiveType(Type.TypeCode.BOOLEAN), Boolean.TRUE,
                    ctx.TRUE().getSymbol().getTokenIndex());
        }
        return Expression.ofUnnamed(booleanValue);
    }

    @Nonnull
    @Override
    public Expression visitBytesLiteral(@Nonnull RelationalParser.BytesLiteralContext ctx) {
        final String literal;
        if (ctx.HEXADECIMAL_LITERAL() != null) {
            literal = ctx.HEXADECIMAL_LITERAL().getText();
        } else {
            literal = ctx.BASE64_LITERAL().getText();
        }
        final byte[] byteArray = ParseHelpers.parseBytes(literal);
        final var value = getDelegate().getPlanGenerationContext().processQueryLiteral(Type.primitiveType(Type.TypeCode.BYTES),
                ZeroCopyByteString.wrap(byteArray), ctx.getStart().getTokenIndex());
        return Expression.ofUnnamed(value);
    }

    @Nonnull
    @Override
    public Expression visitNullLiteral(@Nonnull RelationalParser.NullLiteralContext ctx) {
        return Expression.ofUnnamed(new NullValue(Type.nullType())); // do not strip nulls.
    }

    @Nonnull
    @Override
    public Expression visitStringConstant(@Nonnull RelationalParser.StringConstantContext ctx) {
        return parseChild(ctx);
    }

    @Nonnull
    @Override
    public Expression visitDecimalConstant(@Nonnull RelationalParser.DecimalConstantContext ctx) {
        return parseChild(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNegativeDecimalConstant(@Nonnull RelationalParser.NegativeDecimalConstantContext ctx) {
        return resolveDecimal(ctx.getText(), ctx.getStart().getTokenIndex());
    }

    @Nonnull
    @Override
    public Expression visitBytesConstant(@Nonnull RelationalParser.BytesConstantContext ctx) {
        return parseChild(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBooleanConstant(@Nonnull RelationalParser.BooleanConstantContext ctx) {
        return parseChild(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBitStringConstant(@Nonnull RelationalParser.BitStringConstantContext ctx) {
        Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "bit strings not supported");
        return null;
    }

    @Nonnull
    @Override
    public Expression visitNullConstant(@Nonnull RelationalParser.NullConstantContext ctx) {
        Assert.isNullUnchecked(ctx.NOT(), ErrorCode.UNSUPPORTED_QUERY, "not null is not supported");
        return visitNullLiteral(ctx.nullLiteral());
    }

    /////// Lists //////

    @Nonnull
    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestingsInParens(@Nonnull final RelationalParser.UidListWithNestingsInParensContext ctx) {
        return visitUidListWithNestings(ctx.uidListWithNestings());
    }

    @Nonnull
    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestings(@Nonnull final RelationalParser.UidListWithNestingsContext ctx) {
        final var uidMap = Streams.mapWithIndex(ctx.uidWithNestings().stream(),
                                (ctxWithNesting, index) -> {
                                    final var uid = visitUid(ctxWithNesting.uid());
                                    final var accessor = FieldValue.ResolvedAccessor.of(uid.getName(), (int)index, Type.any());
                                    if (ctxWithNesting.uidListWithNestingsInParens() == null) {
                                        return NonnullPair.of(accessor, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode.of(Type.any(), null));
                                    } else {
                                        return NonnullPair.of(accessor, visitUidListWithNestingsInParens(ctxWithNesting.uidListWithNestingsInParens()));
                                    }
                                })
                        .collect(ImmutableMap.toImmutableMap(NonnullPair::getLeft, NonnullPair::getRight,
                                (l, r) -> {
                                    throw Assert.failUnchecked(ErrorCode.AMBIGUOUS_COLUMN, "duplicate column '" + l + "'");
                                }));
        return CompatibleTypeEvolutionPredicate.FieldAccessTrieNode.of(Type.any(), uidMap);
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructorForInsert(@Nonnull RelationalParser.RecordConstructorForInsertContext ctx) {
        final var expressions = parseRecordFieldsUnderReorderings(ctx.expressionWithOptionalName());
        return Expression.ofUnnamed(RecordConstructorValue.ofColumns(expressions.underlyingAsColumns()));
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructorForInlineTable(@Nonnull RelationalParser.RecordConstructorForInlineTableContext ctx) {
        final var expressions = parseRecordFieldsUnderReorderings(ctx.expressionWithOptionalName());
        return Expression.ofUnnamed(RecordConstructorValue.ofColumns(expressions.underlyingAsColumns()));
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructor(@Nonnull RelationalParser.RecordConstructorContext ctx) {
        if (ctx.uid() != null) {
            final var id = visitUid(ctx.uid());
            if (ctx.STAR() == null) {
                final var expression = getDelegate().getSemanticAnalyzer().resolveIdentifier(id, getDelegate().getCurrentPlanFragment());
                final var resultValue = RecordConstructorValue.ofUnnamed(List.of(expression.getUnderlying()));
                return expression.withUnderlying(resultValue);
            } else {
                final var star = getDelegate().getSemanticAnalyzer().expandStar(Optional.of(id), getDelegate().getLogicalOperators());
                final var resultValue = star.getUnderlying();
                return Expression.ofUnnamed(resultValue);
            }
        }
        if (ctx.STAR() != null) {
            final var star = getDelegate().getSemanticAnalyzer().expandStar(Optional.empty(), getDelegate().getLogicalOperators());
            final var resultValue = star.getUnderlying();
            return Expression.ofUnnamed(resultValue);
        }
        final var expressions = parseRecordFieldsUnderReorderings(ctx.expressionWithOptionalName());
        if (ctx.ofTypeClause() != null) {
            final var recordId = visitUid(ctx.ofTypeClause().uid());
            final var resultValue = RecordConstructorValue.ofColumnsAndName(expressions.underlyingAsColumns(), recordId.getName());
            return Expression.ofUnnamed(resultValue);
        }
        final var resultValue = RecordConstructorValue.ofColumns(expressions.underlyingAsColumns());
        return Expression.ofUnnamed(resultValue);
    }

    @Nonnull
    @Override
    public Expression visitArrayConstructor(@Nonnull RelationalParser.ArrayConstructorContext ctx) {
        final var maybeState = getStateMaybe();
        final var targetTypeMaybe = maybeState.flatMap(LogicalPlanFragment.State::getTargetType);

        if (ctx.expressions() == null) {
            final var elementTypeMaybe = targetTypeMaybe.map(type -> Assert.castUnchecked(type, Type.Array.class).getElementType());
            return Expression.ofUnnamed(elementTypeMaybe.map(AbstractArrayConstructorValue.LightArrayConstructorValue::emptyArray)
                    .orElse(AbstractArrayConstructorValue.LightArrayConstructorValue.emptyArrayOfNone()));
        }

        if (targetTypeMaybe.isEmpty()) {
            return handleArray(ctx);
        }

        final var arrayTargetType = Assert.castUnchecked(targetTypeMaybe.get(), Type.Array.class);
        final var newStateBuilder = LogicalPlanFragment.State.newBuilder().withTargetType(Assert.notNullUnchecked(arrayTargetType.getElementType()));
        try {
            getDelegate().getCurrentPlanFragment().setState(newStateBuilder.build());
            return handleArray(ctx);
        } finally {
            getDelegate().getCurrentPlanFragment().setStateMaybe(maybeState);
        }
    }

    @Nonnull
    private Expressions parseRecordFields(@Nonnull List<? extends ParserRuleContext> parserRuleContexts,
                                          @Nullable List<Type.Record.Field> targetFields) {
        Assert.thatUnchecked(targetFields == null || targetFields.size() == parserRuleContexts.size());
        final var resultsBuilder = ImmutableList.<Expression>builder();
        for (int i = 0; i < parserRuleContexts.size(); i++) {
            final var parserRuleContext = parserRuleContexts.get(i);
            final var targetField = targetFields == null ? null : targetFields.get(i);
            resultsBuilder.add(parseRecordField(parserRuleContext, targetField));
        }
        return Expressions.of(resultsBuilder.build());
    }

    @Nonnull
    private Expression parseRecordField(@Nonnull ParserRuleContext parserRuleContext,
                                        @Nullable Type.Record.Field targetField) {
        final var fieldType = targetField == null ? null : targetField.getFieldType();
        StringTrieNode reorderings = null;
        final var maybeState = getStateMaybe();
        if (targetField != null && maybeState.isPresent() && maybeState.get().getTargetTypeReorderings().isPresent()) {
            reorderings = maybeState.get().getTargetTypeReorderings().get();
        }
        final var targetFieldReorderings = (reorderings == null || reorderings.getChildrenMap() == null) ?
                null :
                reorderings.getChildrenMap().get(targetField.getFieldName());
        final var newStateBuilder = LogicalPlanFragment.State.newBuilder();
        final Expression expression;
        try {
            if (fieldType != null) {
                newStateBuilder.withTargetType(fieldType);
            }
            if (targetFieldReorderings != null && targetFieldReorderings.getChildrenMap() != null) {
                newStateBuilder.withTargetTypeReorderings(targetFieldReorderings);
            }
            getDelegate().getCurrentPlanFragment().setState(newStateBuilder.build());
            expression = Assert.castUnchecked(parserRuleContext.accept(this), Expression.class);
        } finally {
            getDelegate().getCurrentPlanFragment().setStateMaybe(maybeState);
        }
        Assert.notNullUnchecked(expression);
        if (fieldType == null) {
            return expression;
        }
        final var coercedExpression = coerceIfNecessary(expression, fieldType);
        if (expression.getName().isPresent() && targetField.getFieldNameOptional().isPresent()) {
            Assert.thatUnchecked(expression.getName().get().equals(Identifier.of(targetField.getFieldNameOptional().get())));
        }
        if (expression.getName().isEmpty() && targetField.getFieldNameOptional().isPresent()) {
            return coercedExpression.withName(Identifier.of(targetField.getFieldName()));
        }
        return coercedExpression;
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Expression coerceIfNecessary(@Nonnull Expression expression,
                                                @Nonnull Type targetType) {
        final var value = expression.getUnderlying();
        final var maybeCoercedValue = coerceValueIfNecessary(expression.getUnderlying(), targetType);
        if (value != maybeCoercedValue) {
            return new Expression(expression.getName(), DataTypeUtils.toRelationalType(maybeCoercedValue.getResultType()), maybeCoercedValue);
        } else {
            return expression;
        }
    }

    @Nonnull
    private static Value coerceValueIfNecessary(@Nonnull Value value, @Nonnull Type targetType) {
        final var resultType = value.getResultType();
        if (resultType.isUnresolved() ||
                (resultType.isPrimitive() && PromoteValue.isPromotionNeeded(resultType, targetType))) {
            return PromoteValue.inject(value, targetType);
        }
        if (resultType.isArray() && PromoteValue.isPromotionNeeded(resultType, targetType) && value instanceof AbstractArrayConstructorValue) {
            Assert.thatUnchecked(targetType.isArray(), "Cannot convert array type to non-array type");
            final var targetElementType = ((Type.Array) targetType).getElementType();
            return AbstractArrayConstructorValue.LightArrayConstructorValue.of(Streams.stream(value.getChildren()).map(c -> coerceValueIfNecessary(c, targetElementType)).collect(Collectors.toList()));
        }
        return value;
    }

    @Nonnull
    private Expressions parseRecordFieldsUnderReorderings(@Nonnull final List<? extends ParserRuleContext> providedColumnContexts) {
        final var maybeState = getStateMaybe();
        if (maybeState.isEmpty() || maybeState.get().getTargetType().isEmpty()) {
            return parseRecordFields(providedColumnContexts, null);
        }

        final var state = maybeState.get();
        final var targetType = Assert.castUnchecked(state.getTargetType().get(), Type.Record.class);
        final var elementFields = Assert.notNullUnchecked(targetType.getFields());

        if (state.getTargetTypeReorderings().isPresent()) {
            final var targetTypeReorderings = ImmutableList.copyOf(Assert.notNullUnchecked(
                    state.getTargetTypeReorderings().get().getChildrenMap()).keySet());
            final var resultColumnsBuilder = ImmutableList.<Expression>builder();
            Assert.thatUnchecked(targetTypeReorderings.size() >= providedColumnContexts.size(), ErrorCode.SYNTAX_ERROR, "Too many parameters");
            for (final var elementField : elementFields) {
                final int index = targetTypeReorderings.indexOf(elementField.getFieldName());
                final var fieldType = elementField.getFieldType();
                Expression currentFieldColumns = null;
                if (index >= 0 && index < providedColumnContexts.size()) {
                    currentFieldColumns = parseRecordField(providedColumnContexts.get(index), elementField);
                } else if (index >= providedColumnContexts.size()) {
                    // column is declared but the value is not provided
                    Assert.failUnchecked(ErrorCode.SYNTAX_ERROR, "Value of column \"" + elementField.getFieldName() + "\" is not provided");
                } else {
                    // We do not yet support default values for any types, hence it makes sense to simply fail if the field type
                    // expects non-null but no value is provided.
                    Assert.thatUnchecked(fieldType.isNullable(), ErrorCode.NOT_NULL_VIOLATION, "null value in column \"" + elementField.getFieldName() + "\" violates not-null constraint");
                    currentFieldColumns = Expression.fromUnderlying(new NullValue(fieldType));
                }
                resultColumnsBuilder.add(currentFieldColumns);
            }
            return Expressions.of(resultColumnsBuilder.build());
        }

        Assert.thatUnchecked(elementFields.size() == providedColumnContexts.size(),
                ErrorCode.CANNOT_CONVERT_TYPE, "provided record cannot be assigned as its type is incompatible with the target type"
        );
        return parseRecordFields(providedColumnContexts, elementFields);
    }

    @Nonnull
    @Override
    public Expressions visitUpdatedElement(@Nonnull RelationalParser.UpdatedElementContext ctx) {
        final var targetExpression = visitFullColumnName(ctx.fullColumnName());
        final var updateExpression = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
        return Expressions.of(ImmutableList.of(targetExpression, updateExpression));
    }

    @Nonnull
    private Expression handleArray(@Nonnull RelationalParser.ArrayConstructorContext ctx) {
        final var elements = visitExpressions(ctx.expressions()).underlying();

        //
        // TODO This absolutely must call the encapsulator to create the array constructor. The reason being that
        //      we cannot otherwise guarantee that the proper promotions get injected BEFORE the array is constructed.
        //
        //        final var arrayFunctionOptional = FunctionCatalog.resolve("array", elementValues.size());
        //        Assert.thatUnchecked(arrayFunctionOptional.isPresent());
        //        final var arrayFunction = arrayFunctionOptional.get();
        //        return arrayFunction.encapsulate(elementValues);
        //
        // TODO The commented out code does not work yet as we cannot properly compute the max type over complicated
        //      records. Fix that!
        //
        return Expression.ofUnnamed(AbstractArrayConstructorValue
                .LightArrayConstructorValue.of(Streams.stream(elements).collect(ImmutableList.toImmutableList())));
    }

    @Nonnull
    Optional<LogicalPlanFragment.State> getStateMaybe() {
        return getDelegate().getCurrentPlanFragmentMaybe().flatMap(LogicalPlanFragment::getStateMaybe);
    }

    @Nonnull
    private Expression parseChild(@Nonnull ParserRuleContext context) {
        return Assert.castUnchecked(visitChildren(context), Expression.class);
    }

    @Nonnull
    private Expression resolveDecimal(@Nonnull String decimalText, int tokenIndex) {
        final var literal = ParseHelpers.parseDecimal(decimalText);
        final var type = Type.fromObject(literal);
        final var value = getDelegate().getPlanGenerationContext().processQueryLiteral(type, literal, tokenIndex);
        return Expression.ofUnnamed(value);
    }
}
