/*
 * ExpressionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CastValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConditionSelectorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ExistsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RowNumberValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowedValue;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.ColumnarValue;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalPlanFragment;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.WindowSpecExpression;
import com.apple.foundationdb.relational.recordlayer.query.ParseHelpers;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.StringTrieNode;
import com.apple.foundationdb.relational.recordlayer.query.TautologicalValue;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.ZeroCopyByteString;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.jspecify.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import static com.apple.foundationdb.relational.generated.RelationalParser.FunctionArgsContext;

/**
 * This visits expression tree parse nodes and generates a corresponding {@link Expression}.
 */
@API(API.Status.EXPERIMENTAL)
public final class ExpressionVisitor extends DelegatingVisitor<BaseVisitor> {

    private ExpressionVisitor(BaseVisitor baseVisitor) {
        super(baseVisitor);
    }

    public static ExpressionVisitor of(BaseVisitor baseVisitor) {
        return new ExpressionVisitor(baseVisitor);
    }

    @Override
    public LogicalOperator visitTableFunction(RelationalParser.TableFunctionContext ctx) {
        final var functionName = visitTableFunctionName(ctx.tableFunctionName());
        final var arguments = ctx.namedOrUnnamedFunctionArgs() == null ?
                              Expressions.empty() :
                              Assert.castUnchecked(visit(ctx.namedOrUnnamedFunctionArgs()), Expressions.class);
        return getDelegate().resolveTableValuedFunction(functionName, arguments);
    }

    @Override
    public Expressions visitNamedOrUnnamedFunctionArgs(RelationalParser.NamedOrUnnamedFunctionArgsContext ctx) {
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
    public Expression visitNamedFunctionArg(final RelationalParser.NamedFunctionArgContext ctx) {
        final var name = visitUid(ctx.key);
        final var expression = Assert.castUnchecked(visit(ctx.value), Expression.class);
        return expression.toNamedArgument(name);
    }

    @Override
    public Expression visitContinuationAtom(RelationalParser.ContinuationAtomContext ctx) {
        return getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() -> {
            final var continuationExpression = parseChild(ctx);
            SemanticAnalyzer.validateContinuation(continuationExpression);
            return continuationExpression;
        });
    }

    @Override
    public Expression visitSelectStarElement(RelationalParser.SelectStarElementContext ignored) {
        return getDelegate().getSemanticAnalyzer().expandStar(Optional.empty(), getDelegate().getLogicalOperators());
    }

    @Override
    public Expression visitSelectQualifierStarElement(RelationalParser.SelectQualifierStarElementContext ctx) {
        final var identifier = visitUid(ctx.uid());
        // the semantics of valid correlations are extended to expanding a (correlated) qualified star.
        return getDelegate().getSemanticAnalyzer().expandStar(Optional.of(identifier), getDelegate().getLogicalOperatorsIncludingOuter());
    }

    @Override
    public Expression visitFullColumnNameExpressionAtom(RelationalParser.FullColumnNameExpressionAtomContext fullColumnNameExpressionAtomContext) {
        return Assert.castUnchecked(fullColumnNameExpressionAtomContext.fullColumnName().accept(this), Expression.class);
    }

    @Override
    public Expressions visitSelectElements(RelationalParser.SelectElementsContext selectElementsContext) {
        final var selectElements = Expressions.of(selectElementsContext.selectElement().stream()
                .map(selectElement -> Assert.castUnchecked(selectElement.accept(this), Expression.class))
                .collect(ImmutableList.toImmutableList()));

        Assert.thatUnchecked(
                selectElements.stream().noneMatch(
                        exp -> exp.getDataType().getCode() == DataType.Code.ARRAY &&
                                ((DataType.ArrayType)exp.getDataType()).getElementType().getCode() == DataType.Code.ARRAY),
                ErrorCode.UNSUPPORTED_OPERATION,
                "nested arrays are not supported");

        return selectElements;
    }

    @Override
    public Expression visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext selectExpressionElementContext) {
        final var expression = Assert.castUnchecked(selectExpressionElementContext.expression().accept(this), Expression.class);
        if (selectExpressionElementContext.AS() != null) {
            final var expressionName = visitUid(selectExpressionElementContext.uid());
            return expression.withName(expressionName);
        }
        return expression;
    }

    @Override
    public Expression visitFullColumnName(RelationalParser.FullColumnNameContext fullColumnNameContext) {
        final var id = visitFullId(fullColumnNameContext.fullId());
        return getDelegate().getSemanticAnalyzer().resolveIdentifier(id, getDelegate().getCurrentPlanFragment());
    }

    @Override
    public List<OrderByExpression> visitOrderByClause(RelationalParser.OrderByClauseContext orderByClauseContextContext) {
        if (!getDelegate().isTopLevel()) {
            Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "order by is not supported in subquery");
        }
        final List<OrderByExpression> exprs = visitOrderByExpressions(orderByClauseContextContext.orderByExpression());
        getDelegate().getSemanticAnalyzer().validateOrderByColumns(exprs);
        return exprs;
    }

    /**
     * Visits the individual expressions of an {@code ORDER BY} clause.
     */
    private List<OrderByExpression> visitOrderByExpressions(
            List<RelationalParser.OrderByExpressionContext> contexts) {
        return contexts.stream()
                .map(this::visitOrderByExpression)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public OrderByExpression visitOrderByExpression(RelationalParser.OrderByExpressionContext orderByExpressionContext) {
        final var expression = Assert.castUnchecked(orderByExpressionContext.expression().accept(this), Expression.class);
        final var descending = ParseHelpers.isDescending(orderByExpressionContext.orderClause());
        final var nullsLast = ParseHelpers.isNullsLast(orderByExpressionContext.orderClause(), descending);
        return OrderByExpression.of(expression, descending, nullsLast);
    }

    @Override
    public NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> visitInlineTableDefinition(RelationalParser.InlineTableDefinitionContext ctx) {
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

    private static RecordLayerColumn toColumn(FieldValue.ResolvedAccessor field, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode columnIdTrie) {
        // field is always constructed from an Identifier's name (see visitUidListWithNestings), which is never
        // null, even though ResolvedAccessor.getName() is declared @Nullable in general.
        final var columnName = Objects.requireNonNull(field.getName(), "inline table column must have a name");
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

    @Override
    public Expressions visitGroupByClause(RelationalParser.GroupByClauseContext groupByClauseContext) {
        return Expressions.of(groupByClauseContext.groupByItem().stream().map(this::visitGroupByItem).collect(ImmutableList.toImmutableList()));
    }

    @Override
    public Expression visitGroupByItem(RelationalParser.GroupByItemContext groupByItemContext) {
        Assert.isNullUnchecked(groupByItemContext.order, ErrorCode.UNSUPPORTED_QUERY, "ordering grouping column is not supported");
        final var expression = Assert.castUnchecked(groupByItemContext.expression().accept(this), Expression.class);
        if (groupByItemContext.uid() != null) {
            final var name = visitUid(groupByItemContext.uid());
            return expression.withName(name).asEphemeral();
        }
        return expression;
    }

    @Override
    public Expression visitNonAggregateFunctionCall(final RelationalParser.NonAggregateFunctionCallContext ctx) {
        return getDelegate().visitNonAggregateWindowedFunction(ctx.nonAggregateWindowedFunction());
    }

    @Override
    public Expression visitNonAggregateWindowedFunction(final RelationalParser.NonAggregateWindowedFunctionContext windowedFunctionContext) {
        final String functionName = windowedFunctionContext.functionName.getText();
        final WindowSpecExpression windowSpecExpression = getDelegate().visitOverClause(windowedFunctionContext.overClause());

        final var orderByExpressions = windowSpecExpression.getOrderByExpressions();
        final var allowedSortSpecs = StreamSupport.stream(orderByExpressions.spliterator(), false)
                .allMatch(exp -> exp.toSortOrder() == OrderingPart.RequestedSortOrder.ASCENDING || exp.toSortOrder() == OrderingPart.RequestedSortOrder.ANY);
        Assert.thatUnchecked(allowedSortSpecs, ErrorCode.UNSUPPORTED_SORT, "provided sort specification not supported with window function");

        // The partitioning and ordering columns are carried on the window specification; the window options (e.g.
        // ef_search) are carried on the call-site arguments' options map. The window function itself takes no direct
        // positional arguments.
        return getDelegate().getSemanticAnalyzer()
                .resolveWindowFunction(functionName, true, windowSpecExpression, Expressions.empty());
    }

    @Override
    public WindowSpecExpression visitOverClause(final RelationalParser.OverClauseContext ctx) {
        Assert.isNullUnchecked(ctx.windowName(), ErrorCode.UNSUPPORTED_QUERY, "named window functions not supported");

        final var partitionClause = ctx.windowSpec().partitionClause();
        final Expressions partitions = partitionClause == null ? Expressions.empty() : getDelegate().visitPartitionClause(partitionClause);

        final var orderByClause = ctx.windowSpec().orderByClause();
        // Parse ORDER BY expressions directly — the isTopLevel() check in visitOrderByClause
        // is for query-level ORDER BY and does not apply inside OVER clauses.
        final List<OrderByExpression> orderByExpressions = orderByClause == null
                  ? ImmutableList.of()
                  : visitOrderByExpressions(orderByClause.orderByExpression());

        final var windowOptionsClause = ctx.windowSpec().windowOptionsClause();
        final Expressions windowOptions = windowOptionsClause == null ? Expressions.empty() : getDelegate().visitWindowOptionsClause(windowOptionsClause);

        return WindowSpecExpression.of(partitions, orderByExpressions, windowOptions);
    }

    @Override
    public Expressions visitWindowOptionsClause(final RelationalParser.WindowOptionsClauseContext ctx) {
        return Expressions.of(ImmutableSet.copyOf(ctx.windowOption().stream().map(option -> getDelegate().visitWindowOption(option))
                .collect(ImmutableList.toImmutableList())));
    }

    @Override
    public Expressions visitPartitionClause(final RelationalParser.PartitionClauseContext ctx) {
        final var partitionByExpressions = ctx.fullId().stream()
                .map(fullId -> getDelegate().getSemanticAnalyzer()
                        .resolveIdentifier(visitFullId(fullId), getDelegate().getCurrentPlanFragment()))
                .collect(ImmutableList.toImmutableList());
        return Expressions.of(partitionByExpressions);
    }

    @Override
    public Expression visitWindowOption(final RelationalParser.WindowOptionContext ctx) {
        if (ctx.EF_SEARCH() != null) {
            //
            // The literal is passed on as parsed; the option's declared type is enforced when the window function is
            // encapsulated, so an out-of-range value is reported as a bad option value rather than an internal error.
            //
            final var value = LiteralValue.ofScalar(ParseHelpers.parseDecimal(ctx.efSearch.getText()));
            return Expression.of(value, Identifier.of(RowNumberValue.RowNumberFn.EF_SEARCH.getName()));
        }
        throw Assert.failUnchecked(ErrorCode.INTERNAL_ERROR, "unexpected option " + ctx.getText());
    }

    @Override
    public Expression visitAggregateFunctionCall(RelationalParser.AggregateFunctionCallContext functionCon) {
        return visitAggregateWindowedFunction(functionCon.aggregateWindowedFunction());
    }

    @Override
    public Expression visitAggregateWindowedFunction(RelationalParser.AggregateWindowedFunctionContext functionContext) {
        final Token functionName = functionContext.functionName;
        final String name = functionName.getText();

        // Handle the aggregator (aka. set quantifier). Existing aggregates support only ALL (the default).
        final Token aggregator = functionContext.aggregator;
        Assert.thatUnchecked(
                aggregator == null || aggregator.getType() == RelationalParser.ALL,
                ErrorCode.UNSUPPORTED_QUERY,
                () -> String.format(Locale.ROOT, "aggregator %s is not supported",
                        Assert.notNullUnchecked(aggregator).getText()));

        // Handle the OVER clause. The grammar admits it for most aggregates, but using an aggregate as a window
        // function is not implemented.
        Assert.isNullUnchecked(
                functionContext.overClause(),
                ErrorCode.UNSUPPORTED_QUERY,
                String.format(Locale.ROOT, "an OVER clause is not supported for %s()", name));

        // Handle the null treatment clause. The grammar admits it for ARRAY_AGG() only. Absent a clause, the default is
        // RESPECT NULLS (which is aligned with the SQL standard).
        final RelationalParser.NullTreatmentClauseContext nullTreatment = functionContext.nullTreatmentClause();
        final boolean ignoreNulls = nullTreatment != null && getDelegate().visitNullTreatmentClause(nullTreatment);

        // Determine and visit the arguments.
        // * For the star argument of COUNT(*), use an empty record as a stand-in.
        // * Multiple arguments are admitted for COUNT(DISTINCT …) and GROUP_CONCAT(), but not implemented yet. Note
        //   that GROUP_CONCAT() uses the multi-argument production even when it is given a single argument.
        final ImmutableList.Builder<Expression> args = ImmutableList.builder();
        if (functionContext.starArg != null) {
            args.add(Expression.ofUnnamed(RecordConstructorValue.ofColumns(List.of())));
        } else if (functionContext.functionArgs() != null) {
            final List<RelationalParser.FunctionArgContext> functionArgs = functionContext.functionArgs().functionArg();
            Assert.thatUnchecked(
                    functionArgs.size() == 1,
                    ErrorCode.UNSUPPORTED_QUERY,
                    () -> String.format(Locale.ROOT, "multiple arguments are not supported for %s()", name));
            args.add(visitFunctionArg(functionArgs.get(0)));
        } else if (functionContext.functionArg() != null) {
            args.add(visitFunctionArg(functionContext.functionArg()));
        }
        // ARRAY_AGG() carries the null treatment as an extra literal argument, to be consumed by its encapsulation.
        if (functionName.getType() == RelationalParser.ARRAY_AGG) {
            args.add(Expression.ofUnnamed(LiteralValue.ofScalar(ignoreNulls)));
        }
        final Expressions arguments = Expressions.of(args.build());

        // Handle the in-call ORDER BY clause. The grammar admits it for ARRAY_AGG() and GROUP_CONCAT(), but they do
        // not honor it yet. We still visit the sort expressions, so that they undergo the usual semantic analysis.
        // This happens only once the arguments have been visited, so an error in an argument takes precedence.
        final RelationalParser.OrderByClauseContext orderByClause = functionContext.orderByClause();
        if (orderByClause != null) {
            visitOrderByExpressions(orderByClause.orderByExpression());
            throw Assert.failUnchecked(
                    ErrorCode.UNSUPPORTED_QUERY,
                    String.format(Locale.ROOT, "an ORDER BY clause is not supported for %s()", name));
        }

        // Resolve the function.
        return getDelegate().resolveFunction(name, arguments);
    }

    @Override
    public Boolean visitNullTreatmentClause(final RelationalParser.NullTreatmentClauseContext ctx) {
        return ctx.nullTreatment.getType() == RelationalParser.IGNORE;
    }

    @Override
    public Expression visitScalarFunctionCall(RelationalParser.ScalarFunctionCallContext ctx) {
        final var functionName = ctx.scalarFunctionName().getText();
        // special case for user-defined functions where we want to exclude the first argument from
        // being literal-stripped.
        Expressions arguments;
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

    @Override
    public Expression visitUserDefinedScalarFunctionCall(RelationalParser.UserDefinedScalarFunctionCallContext ctx) {
        final var functionName = Identifier.of(getDelegate().normalizeString(ctx.userDefinedScalarFunctionName().getText()));
        Expressions arguments = ctx.namedOrUnnamedFunctionArgs() == null ?
                                Expressions.empty() :
                                Assert.castUnchecked(visit(ctx.namedOrUnnamedFunctionArgs()), Expressions.class);
        return getDelegate().resolveFunction(functionName.getName(), arguments.asList().toArray(new Expression[0]));
    }

    @Override
    public Expression visitCaseFunctionCall(RelationalParser.CaseFunctionCallContext ctx) {
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

    @Override
    public Expression visitDataTypeFunctionCall(RelationalParser.DataTypeFunctionCallContext ctx) {
        if (ctx.CAST() != null) {
            final var sourceExpression = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
            final var isRepeated = ctx.convertedDataType().ARRAY() != null;
            final var typeInfo = SemanticAnalyzer.ParsedTypeInfo.ofPrimitiveType(ctx.convertedDataType().typeName, false, isRepeated);
            // Cast does not currently support user-defined struct types.
            final var targetDataType = getDelegate().getSemanticAnalyzer().lookupBuiltInType(typeInfo);
            final var targetType = DataTypeUtils.toRecordLayerType(targetDataType);
            final var underlyingType = sourceExpression.getUnderlying().getResultType();
            // Note: `inject()` does not necessarily return a `CastValue`.
            final var value = CastValue.inject(sourceExpression.getUnderlying(), targetType.withNullability(underlyingType.isNullable()));
            return Expression.ofUnnamed(targetDataType, value);
        }

        throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "CONVERT function is not yet supported");
    }

    @Override
    public Expression visitFunctionCallExpressionAtom(RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return parseChild(ctx);
    }

    @Override
    public Expression visitFunctionArg(RelationalParser.FunctionArgContext functionArgContext) {
        return Assert.castUnchecked(functionArgContext.expression().accept(this), Expression.class);
    }

    @Override
    public Expressions visitFunctionArgs(@Nullable FunctionArgsContext ctx) {
        if (ctx == null) {
            return Expressions.of(List.of());
        } else {
            return Expressions.of(ctx.functionArg().stream().map(this::visitFunctionArg).collect(ImmutableList.toImmutableList()));
        }
    }

    @Override
    public Expression visitHavingClause(RelationalParser.HavingClauseContext havingClauseContext) {
        return Assert.castUnchecked(havingClauseContext.expression().accept(this), Expression.class);
    }

    @Override
    public Expression visitPreparedStatementParameterAtom(RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return visitPreparedStatementParameter(ctx.preparedStatementParameter());
    }

    @Override
    public Expression visitPreparedStatementParameter(RelationalParser.PreparedStatementParameterContext ctx) {
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

    @Override
    public Expression visitNotExpression(RelationalParser.NotExpressionContext ctx) {
        final var argument = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.NOT().getText(), argument);
    }

    @Override
    public Expression visitLogicalExpression(RelationalParser.LogicalExpressionContext ctx) {
        final var left = Assert.castUnchecked(ctx.expression(0).accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.expression(1).accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.logicalOperator().getText(), left, right);
    }

    @Override
    public Expression visitPredicatedExpression(final RelationalParser.PredicatedExpressionContext ctx) {
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
        throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "unsupported predicate " + ctx.predicate().getText());
    }

    @Override
    public Expression visitLimitClause(RelationalParser.LimitClauseContext ctx) {
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

    @Override
    public Expression visitLimitClauseAtom(RelationalParser.LimitClauseAtomContext ctx) {
        return parseChild(ctx);
    }

    ///// Predicates ///////

    @Override
    public Expression visitExistsExpressionAtom(RelationalParser.ExistsExpressionAtomContext ctx) {
        /*
         * (yhatem) this is an interesting visitation, as it requires three interactions:
         * - Firstly, LogicalOperator-visitor calls Expression-visitor to visit this predicate.
         * - Secondly, Expression-visitor calls into LogicalOperator to visit the subquery.
         * - Thirdly, Expression-visitor wraps the subquery in Exists predicate and returns
         *   it to LogicalOperator-visitor.
         */
        final var selectOperator = visitQuery(ctx.query());
        final var asExistential = selectOperator.withQuantifier(Quantifier.existential(selectOperator.getQuantifier().getRangesOver()));
        final var underlyingValue = new ExistsValue(QuantifiedObjectValue.of(asExistential.getQuantifier()));
        if (getDelegate().getPlanGenerationContext().shouldProcessLiteral()) {
            getDelegate().getCurrentPlanFragment().addOperator(asExistential);
        }
        return Expression.ofUnnamed(underlyingValue);
    }

    private Expression visitIsExpression(Expression operand, RelationalParser.IsExpressionContext ctx) {
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

    private Expression visitLikePredicate(Expression operand, RelationalParser.LikePredicateContext ctx) {
        final LiteralValue<?> escapeValue;
        if (ctx.escape != null) {
            final var escapeChar = getDelegate().normalizeString(ctx.escape.getText());
            escapeValue = new LiteralValue<>(escapeChar);
        } else {
            escapeValue = new LiteralValue<>(null);
        }
        final var patternValueBinding = Assert.castUnchecked(ctx.pattern.accept(this), Expression.class);
        final var patternFunction = getDelegate().resolveFunction("__pattern_for_like", patternValueBinding,
                Expression.ofUnnamed(escapeValue));
        final var likeFunction = getDelegate().resolveFunction(ctx.LIKE().getText(), operand, patternFunction);
        if (ctx.NOT() != null) {
            return getDelegate().resolveFunction(ctx.NOT().getText(), likeFunction);
        }
        return likeFunction;
    }

    private Expression visitInPredicate(Expression operand, RelationalParser.InPredicateContext ctx) {
        Assert.thatUnchecked(ctx.inList().queryExpressionBody() == null, ErrorCode.UNSUPPORTED_QUERY,
                "IN predicate does not support nested SELECT");
        final var right = visitInList(ctx.inList());
        var in = getDelegate().resolveFunction(ctx.IN().getText(), operand, right);
        if (ctx.NOT() != null) {
            in = getDelegate().resolveFunction(ctx.NOT().getText(), in);
        }
        return in;
    }

    @Override
    public Expression visitInList(RelationalParser.InListContext ctx) {
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

    @Override
    public Expression visitWhereExpr(RelationalParser.WhereExprContext ctx) {
        final var expression = parseChild(ctx);
        // verify no window functions
        Assert.thatUnchecked(expression.getUnderlying().preOrderStream().noneMatch(v -> v instanceof WindowedValue),
                ErrorCode.WINDOWING_ERROR, "window functions are not allowed in WHERE");
        return expression;
    }

    @Override
    public Expression visitQualifyClause(final RelationalParser.QualifyClauseContext ctx) {
        return parseChild(ctx);
    }

    @Override
    public Expressions visitExpressions(RelationalParser.ExpressionsContext ctx) {
        return Expressions.of(ctx.expression()
                .stream()
                .map(expression -> Assert.castUnchecked(expression.accept(this), Expression.class))
                .collect(ImmutableList.toImmutableList()));
    }

    @Override
    public Expression visitBitExpressionAtom(RelationalParser.BitExpressionAtomContext ctx) {
        final var left = Assert.castUnchecked(ctx.left.accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.right.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.bitOperator().getText(), left, right);
    }

    @Override
    public Expression visitBinaryComparisonPredicate(RelationalParser.BinaryComparisonPredicateContext ctx) {
        final var left = Assert.castUnchecked(ctx.left.accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.right.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.comparisonOperator().getText(), left, right);
    }

    @Override
    public Expression visitSubscriptExpression(RelationalParser.SubscriptExpressionContext ctx) {
        final var index = Assert.castUnchecked(ctx.index.accept(this), Expression.class);
        final var base = Assert.castUnchecked(ctx.base.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.LEFT_SQUARE_BRACKET().getText()
                .concat(ctx.RIGHT_SQUARE_BRACKET().getText()), index, base);
    }

    private Expression visitBetweenComparisonPredicate(Expression operand,
                                                       RelationalParser.BetweenComparisonPredicateContext ctx) {
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

    @Override
    public Expression visitMathExpressionAtom(RelationalParser.MathExpressionAtomContext ctx) {
        final var left = Assert.castUnchecked(ctx.left.accept(this), Expression.class);
        final var right = Assert.castUnchecked(ctx.right.accept(this), Expression.class);
        return getDelegate().resolveFunction(ctx.mathOperator().getText(), left, right);
    }

    @Override
    public Expression visitExpressionWithOptionalName(RelationalParser.ExpressionWithOptionalNameContext ctx) {
        final var expression = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
        if (ctx.AS() != null) {
            final var name = visitUid(ctx.uid());
            return expression.withName(name);
        }
        return expression;
    }

    /////// Literals and Constants //////

    @Override
    public Expression visitDecimalLiteral(RelationalParser.DecimalLiteralContext ctx) {
        return resolveDecimal(ctx.getText(), ctx.getStart().getTokenIndex());
    }

    @Override
    public Expression visitStringLiteral(RelationalParser.StringLiteralContext ctx) {
        Assert.isNullUnchecked(ctx.STRING_CHARSET_NAME(), ErrorCode.UNSUPPORTED_QUERY, "charset not is supported");
        Assert.isNullUnchecked(ctx.START_NATIONAL_STRING_LITERAL(), ErrorCode.UNSUPPORTED_QUERY, "national string literal is not supported");
        Assert.isNullUnchecked(ctx.COLLATE(), ErrorCode.UNSUPPORTED_QUERY, "collation is not supported");
        final var value = getDelegate().getPlanGenerationContext().processQueryLiteral(Type.primitiveType(Type.TypeCode.STRING),
                getDelegate().normalizeString(ctx.getText()),
                ctx.getStart().getTokenIndex());
        return Expression.ofUnnamed(value);
    }

    @Override
    public Expression visitBooleanLiteral(RelationalParser.BooleanLiteralContext ctx) {
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

    @Override
    public Expression visitBytesLiteral(RelationalParser.BytesLiteralContext ctx) {
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

    @Override
    public Expression visitNullLiteral(RelationalParser.NullLiteralContext ctx) {
        return Expression.ofUnnamed(new NullValue(Type.nullType())); // do not strip nulls.
    }

    @Override
    public Expression visitStringConstant(RelationalParser.StringConstantContext ctx) {
        return parseChild(ctx);
    }

    @Override
    public Expression visitDecimalConstant(RelationalParser.DecimalConstantContext ctx) {
        return parseChild(ctx);
    }

    @Override
    public Expression visitNegativeDecimalConstant(RelationalParser.NegativeDecimalConstantContext ctx) {
        return resolveDecimal(ctx.getText(), ctx.getStart().getTokenIndex());
    }

    @Override
    public Expression visitBytesConstant(RelationalParser.BytesConstantContext ctx) {
        return parseChild(ctx);
    }

    @Override
    public Expression visitBooleanConstant(RelationalParser.BooleanConstantContext ctx) {
        return parseChild(ctx);
    }

    @Override
    public Expression visitBitStringConstant(RelationalParser.BitStringConstantContext ctx) {
        throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "bit strings not supported");
    }

    @Override
    public Expression visitNullConstant(RelationalParser.NullConstantContext ctx) {
        Assert.isNullUnchecked(ctx.NOT(), ErrorCode.UNSUPPORTED_QUERY, "not null is not supported");
        return visitNullLiteral(ctx.nullLiteral());
    }

    /////// Lists //////

    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestingsInParens(final RelationalParser.UidListWithNestingsInParensContext ctx) {
        return visitUidListWithNestings(ctx.uidListWithNestings());
    }

    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestings(final RelationalParser.UidListWithNestingsContext ctx) {
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
                            throw Assert.failUnchecked(ErrorCode.AMBIGUOUS_COLUMN, "duplicate column " + l);
                        }));
        return CompatibleTypeEvolutionPredicate.FieldAccessTrieNode.of(Type.any(), uidMap);
    }

    @Override
    public Expression visitRecordConstructorForInsert(RelationalParser.RecordConstructorForInsertContext ctx) {
        final var expressions = parseRecordFieldsUnderReorderings(ctx.expressionWithOptionalName());
        return Expression.ofUnnamed(RecordConstructorValue.ofColumns(expressions.underlyingAsColumns()));
    }

    @Override
    public Expression visitRecordConstructorForInlineTable(RelationalParser.RecordConstructorForInlineTableContext ctx) {
        final var expressions = parseRecordFieldsUnderReorderings(ctx.expressionWithOptionalName());
        return Expression.ofUnnamed(RecordConstructorValue.ofColumns(expressions.underlyingAsColumns()));
    }

    @Override
    public Expression visitRecordConstructor(RelationalParser.RecordConstructorContext ctx) {
        if (ctx.uid() != null) {
            final var id = visitUid(ctx.uid());
            if (ctx.STAR() == null) {
                final var expression = getDelegate().getSemanticAnalyzer().resolveIdentifier(id, getDelegate().getCurrentPlanFragment());
                final var resultValue = RecordConstructorValue.ofUnnamed(List.of(expression.getUnderlying()));
                return expression.withUnderlying(resultValue);
            } else {
                final var star = getDelegate().getSemanticAnalyzer().expandStar(Optional.of(id), getDelegate().getLogicalOperators());
                final var resultValue = star.getUnderlying();
                // Name the column after the qualifier (table name or alias) that was expanded.
                return Expression.of(resultValue, id);
            }
        }
        if (ctx.STAR() != null) {
            final var star = getDelegate().getSemanticAnalyzer().expandStar(Optional.empty(), getDelegate().getLogicalOperators());
            final var resultValue = star.getUnderlying();
            // Name the column after the sole for-each quantifier in scope.
            // Both standard joins and PartiQL unnest expansions introduce multiple for-each
            // quantifiers (attributes originating from different sources), so fall back to
            // an unnamed column whenever there is more than one.
            final var forEachOps = getDelegate().getLogicalOperators().forEachOnly();
            if (Iterables.size(forEachOps) == 1) {
                return Iterables.getOnlyElement(forEachOps).getName()
                        .map(name -> Expression.of(resultValue, name))
                        .orElse(Expression.ofUnnamed(resultValue));
            }
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

    @Override
    public Expression visitArrayConstructor(RelationalParser.ArrayConstructorContext ctx) {
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
        // arrayTargetType.getElementType() is @Nullable only for an erased array type, which cannot occur here since
        // the target type was propagated down from a resolved schema/expression type; Assert.notNullUnchecked
        // enforces that invariant at runtime with a clear RelationalException, but NullAway can't see that since
        // Assert lives in the not-yet-migrated fdb-relational-api module.
        @SuppressWarnings("NullAway")
        final var newStateBuilder = LogicalPlanFragment.State.newBuilder().withTargetType(Assert.notNullUnchecked(arrayTargetType.getElementType()));
        try {
            getDelegate().getCurrentPlanFragment().setState(newStateBuilder.build());
            return handleArray(ctx);
        } finally {
            getDelegate().getCurrentPlanFragment().setStateMaybe(maybeState);
        }
    }

    private Expressions parseRecordFields(List<? extends ParserRuleContext> parserRuleContexts,
                                          @Nullable List<Field> targetFields) {
        Assert.thatUnchecked(targetFields == null || targetFields.size() == parserRuleContexts.size());
        final var resultsBuilder = ImmutableList.<Expression>builder();
        for (int i = 0; i < parserRuleContexts.size(); i++) {
            final var parserRuleContext = parserRuleContexts.get(i);
            final var targetField = targetFields == null ? null : targetFields.get(i);
            resultsBuilder.add(parseRecordField(parserRuleContext, targetField));
        }
        return Expressions.of(resultsBuilder.build());
    }

    private Expression parseRecordField(ParserRuleContext parserRuleContext,
                                        @Nullable Field targetField) {
        final var fieldType = targetField == null ? null : targetField.getFieldType();
        StringTrieNode reorderings = null;
        final var maybeState = getStateMaybe();
        if (targetField != null && maybeState.isPresent() && maybeState.get().getTargetTypeReorderings().isPresent()) {
            reorderings = maybeState.get().getTargetTypeReorderings().get();
        }
        // reorderings is only ever assigned (above) when targetField is non-null, so the extra targetField == null
        // check below is redundant at runtime but lets NullAway see that targetField.getFieldName() is safe.
        final var targetFieldReorderings = (targetField == null || reorderings == null || reorderings.getChildrenMap() == null) ?
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
        // fieldType is derived from targetField.getFieldType() above, so fieldType being non-null implies
        // targetField is also non-null; reassign so NullAway can see that for the rest of this method.
        targetField = Objects.requireNonNull(targetField);
        var coercedExpression = coerceIfNecessary(expression, fieldType);
        if (targetField.getFieldIndexOptional().isPresent()) {
            coercedExpression = coercedExpression.withUnderlying(new ColumnarValue(coercedExpression.getUnderlying(),
                    targetField.getFieldIndex()));
        }
        if (expression.getName().isPresent() && targetField.getFieldNameOptional().isPresent()) {
            Assert.thatUnchecked(expression.getName().get().equals(Identifier.of(targetField.getFieldNameOptional().get())));
        }
        if (expression.getName().isEmpty() && targetField.getFieldNameOptional().isPresent()) {
            return coercedExpression.withName(Identifier.of(targetField.getFieldName()));
        }
        return coercedExpression;
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Expression coerceIfNecessary(Expression expression,
                                                Type targetType) {
        final var value = expression.getUnderlying();
        final var maybeCoercedValue = coerceValueIfNecessary(expression.getUnderlying(), targetType);
        if (value != maybeCoercedValue) {
            return new Expression(expression.getName(), DataTypeUtils.toRelationalType(maybeCoercedValue.getResultType()), maybeCoercedValue);
        } else {
            return expression;
        }
    }

    private static Value coerceValueIfNecessary(Value value, Type targetType) {
        final var resultType = value.getResultType();
        if (resultType.isUnresolved() ||
                (resultType.isPrimitive() && PromoteValue.isPromotionNeeded(resultType, targetType))) {
            return PromoteValue.inject(value, targetType);
        }
        if (resultType.isArray() && PromoteValue.isPromotionNeeded(resultType, targetType) && value instanceof AbstractArrayConstructorValue) {
            Assert.thatUnchecked(targetType.isArray(), "Cannot convert array type to non-array type");
            // getElementType() is @Nullable only for an erased array type; targetType here always originates from a
            // resolved schema/expression type, so it is never erased. Assert.notNullUnchecked enforces that
            // invariant at runtime with a clear RelationalException, but NullAway can't see that since Assert lives
            // in the not-yet-migrated fdb-relational-api module.
            @SuppressWarnings("NullAway")
            final Type targetElementType = Assert.notNullUnchecked(((Type.Array) targetType).getElementType());
            return AbstractArrayConstructorValue.LightArrayConstructorValue.of(Streams.stream(value.getChildren()).map(c -> coerceValueIfNecessary(c, targetElementType)).collect(Collectors.toList()));
        }
        return value;
    }

    private Expressions parseRecordFieldsUnderReorderings(final List<? extends ParserRuleContext> providedColumnContexts) {
        final var maybeState = getStateMaybe();
        if (maybeState.isEmpty() || maybeState.get().getTargetType().isEmpty()) {
            return parseRecordFields(providedColumnContexts, null);
        }

        final var state = maybeState.get();
        final var targetType = Assert.castUnchecked(state.getTargetType().get(), Type.Record.class);
        final var elementFields = Assert.notNullUnchecked(targetType.getFields());

        if (state.getTargetTypeReorderings().isPresent()) {
            // FieldAccessTrieNode.getChildrenMap() is @Nullable in general, but the reorderings trie built for a
            // record target type always carries a non-null children map; Assert.notNullUnchecked enforces that
            // invariant at runtime with a clear RelationalException, but NullAway can't see that since Assert lives
            // in the not-yet-migrated fdb-relational-api module.
            @SuppressWarnings("NullAway")
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
                    throw Assert.failUnchecked(ErrorCode.SYNTAX_ERROR, "Value of column \"" + elementField.getFieldName() + "\" is not provided");
                } else {
                    // We do not yet support default values for any types, hence it makes sense to simply fail if the field type
                    // expects non-null but no value is provided.
                    Assert.thatUnchecked(fieldType.isNullable(), ErrorCode.NOT_NULL_VIOLATION, "null value in column \"" + elementField.getFieldName() + "\" violates not-null constraint");
                    Value value = new NullValue(fieldType);
                    if (elementField.getFieldIndexOptional().isPresent()) {
                        value = new ColumnarValue(value, elementField.getFieldIndex());
                    }
                    currentFieldColumns = Expression.fromUnderlying(value);
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

    @Override
    public Expressions visitUpdatedElement(RelationalParser.UpdatedElementContext ctx) {
        final var targetExpression = visitFullColumnName(ctx.fullColumnName());
        final var updateExpression = Assert.castUnchecked(ctx.expression().accept(this), Expression.class);
        return Expressions.of(ImmutableList.of(targetExpression, updateExpression));
    }

    private Expression handleArray(RelationalParser.ArrayConstructorContext ctx) {
        // Promote the individual array elements to their respective non-nullable types, as arrays cannot currently
        // store NULL elements (Issue #3646). NULL literals are rejected here as UNSUPPORTED_OPERATION, as `NullType`
        // cannot be made non-nullable.
        final Expressions elements = visitExpressions(ctx.expressions());
        final ImmutableList<Expression> promotedElements =
                Streams.stream(elements.underlying())
                        .map(value -> {
                            final Type type = value.getResultType();
                            Assert.thatUnchecked(!type.isNull(), ErrorCode.UNSUPPORTED_OPERATION,
                                    "An ARRAY value cannot have NULL elements");
                            return Expression.fromUnderlying(PromoteValue.inject(value, type.notNullable()));
                        })
                        .collect(ImmutableList.toImmutableList());
        final Expression[] array = promotedElements.toArray(new Expression[0]);
        return getDelegate().resolveFunction("__internal_array", false, array);
    }

    Optional<LogicalPlanFragment.State> getStateMaybe() {
        return getDelegate().getCurrentPlanFragmentMaybe().flatMap(LogicalPlanFragment::getStateMaybe);
    }

    private Expression parseChild(ParserRuleContext context) {
        return Assert.castUnchecked(visitChildren(context), Expression.class);
    }

    private Expression resolveDecimal(String decimalText, int tokenIndex) {
        final var literal = ParseHelpers.parseDecimal(decimalText);
        final var type = Type.fromObject(literal);
        final var value = getDelegate().getPlanGenerationContext().processQueryLiteral(type, literal, tokenIndex);
        return Expression.ofUnnamed(value);
    }
}
