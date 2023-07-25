/*
 * AstVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.NotValue;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.DeleteExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.InsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ExistsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FunctionCatalog;
import com.apple.foundationdb.record.query.plan.cascades.values.InOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LikeOperatorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PatternForLikeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.net.URI;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Visits the abstract syntax tree of the query and generates a {@link com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression}
 * if the query is valid and supported.
 * <br>
 * Note: this does not support array literals yet, we have some ad-hoc support for flat literal arrays for handling in-predicate.
 */
public class AstVisitor extends RelationalParserBaseVisitor<Object> {

    @Nonnull
    private final PlanGenerationContext context;

    @Nonnull
    private final URI dbUri;

    public static final String UNSUPPORTED_QUERY = "query is not supported";

    @Nonnull
    private final DdlQueryFactory ddlQueryFactory;

    /*
    whether the schema contains Non-nullable array.
    If it is false, we'll wrap array field in all indexes. If it is true, we won't wrap any array field in any indexes.
    It is a temporary work-around. We should wrap nullable array field in indexes. It'll be removed after this work is done.
    */
    private boolean containsNonNullableArray;

    @Nonnull
    private final Scopes scopes;

    @Nonnull
    private final String query;

    /**
     * Creates a new instance of {@link AstVisitor}.
     *
     * @param context         The parsing context used to maintain references.
     * @param ddlQueryFactory       A factory used to query the metadata.
     * @param dbUri                 The current database {@link URI}.
     */
    public AstVisitor(@Nonnull final PlanGenerationContext context,
                      @Nonnull final DdlQueryFactory ddlQueryFactory,
                      @Nonnull final URI dbUri,
                      @Nonnull final String query) {
        this.context = context;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
        this.containsNonNullableArray = false;
        this.scopes = new Scopes();
        this.query = query;
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        return nextResult != null ? nextResult : aggregate;
    }

    @Override
    public Object visitFullDescribeStatement(RelationalParser.FullDescribeStatementContext ctx) {
        context.setForExplain(ctx.EXPLAIN() != null);
        return visit(ctx.describeObjectClause());
    }

    @Override
    public QueryPlan visitDmlStatement(RelationalParser.DmlStatementContext ctx) {
        Assert.thatUnchecked(ctx.selectStatementWithContinuation() != null ||
                ctx.insertStatement() != null ||
                ctx.updateStatement() != null ||
                ctx.deleteStatement() != null, UNSUPPORTED_QUERY);

        return (QueryPlan) visitChildren(ctx);
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Typed visitUnionParenthesisSelect(RelationalParser.UnionParenthesisSelectContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Typed visitUnionSelect(RelationalParser.UnionSelectContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public QueryPlan visitSelectStatementWithContinuation(RelationalParser.SelectStatementWithContinuationContext ctx) {
        Assert.notNullUnchecked(ctx.selectStatement(), UNSUPPORTED_QUERY);
        RelationalExpression result = (RelationalExpression) ctx.selectStatement().accept(this);
        if (ctx.CONTINUATION() != null) {
            Assert.thatUnchecked(context.asDql().getOffset() == 0, "Offset cannot be specified with continuation.");
            context.setContinuation((byte[]) visit(ctx.continuationAtom()));
        }
        return QueryPlan.LogicalQueryPlan.of(result, context, query);
    }

    @Override
    public byte[] visitContinuationAtom(RelationalParser.ContinuationAtomContext ctx) {
        if (ctx.stringLiteral() != null) {
            return context.withDisabledLiteralProcessing(() -> {
                final var literal = visitChildren(ctx);
                Assert.thatUnchecked(literal instanceof LiteralValue<?>);
                final var continuationObject = ((LiteralValue<?>) literal).getLiteralValue();
                Assert.thatUnchecked(continuationObject instanceof String);
                final var continuationString = (String) continuationObject;
                Assert.notNullUnchecked(continuationString, "Illegal query with BEGIN continuation.", ErrorCode.INVALID_CONTINUATION);
                Assert.thatUnchecked(!continuationString.isEmpty(), "Illegal query with END continuation.", ErrorCode.INVALID_CONTINUATION);
                return Base64.getDecoder().decode(continuationString);
            });
        } else {
            return context.withDisabledLiteralProcessing(() -> {
                final var literal = visitChildren(ctx);
                Assert.thatUnchecked(literal instanceof LiteralValue<?>);
                final var continuationBytes = ((LiteralValue<?>) literal).getLiteralValue();
                Assert.notNullUnchecked(continuationBytes);
                Assert.thatUnchecked(continuationBytes instanceof byte[],
                        String.format("Unexpected prepared continuation parameter of type %s", continuationBytes.getClass().getSimpleName()),
                        ErrorCode.INVALID_CONTINUATION);
                return (byte[]) continuationBytes;
            });
        }
    }

    @Override
    public RelationalExpression visitParenthesisSelect(RelationalParser.ParenthesisSelectContext ctx) {
        return (RelationalExpression) ctx.queryExpression().accept(this);
    }

    @Override
    public RelationalExpression visitSimpleSelect(RelationalParser.SimpleSelectContext ctx) {
        return (RelationalExpression) ctx.querySpecification().accept(this);
    }

    @ExcludeFromJacocoGeneratedReport // not reachable for now, but planned.
    @Override
    public RelationalExpression visitQueryExpression(RelationalParser.QueryExpressionContext ctx) {
        if (ctx.queryExpression() != null) {
            return (RelationalExpression) ctx.queryExpression().accept(this); // recursive
        }
        return (RelationalExpression) ctx.querySpecification().accept(this);
    }

    @Override
    public RelationalExpression visitQuerySpecification(RelationalParser.QuerySpecificationContext ctx) {
        //        Assert.thatUnchecked(ctx.selectSpec().isEmpty(), UNSUPPORTED_QUERY);
//        Assert.isNullUnchecked(ctx.windowClause(), UNSUPPORTED_QUERY);
        Assert.notNullUnchecked(ctx.fromClause(), UNSUPPORTED_QUERY);

        return handleSelectInternal(ctx.selectElements(), ctx.fromClause(), ctx.groupByClause(), ctx.havingClause(), ctx.orderByClause(), ctx.limitClause());
    }

    private RelationalExpression handleSelectInternal(@Nonnull final RelationalParser.SelectElementsContext selectElements,
                                                      @Nonnull final RelationalParser.FromClauseContext fromClause,
                                                      @Nullable final RelationalParser.GroupByClauseContext groupByClause,
                                                      @Nullable final RelationalParser.HavingClauseContext havingClauseContext,
                                                      @Nullable final RelationalParser.OrderByClauseContext orderByClause,
                                                      @Nullable final RelationalParser.LimitClauseContext limitClause) {
        if (groupByClause != null || ParserUtils.hasAggregation(selectElements)) {
            return handleSelectWithGroupBy(selectElements, fromClause, groupByClause, havingClauseContext, orderByClause, limitClause);
        } else {
            scopes.sibling();
            fromClause.accept(this); // includes checking predicates
            selectElements.accept(this); // potentially sets explicit result columns on top-level select expression.
            if (orderByClause != null) {
                visit(orderByClause);
            }
            if (limitClause != null) {
                visit(limitClause);
            }
            return scopes.pop().convertToRelationalExpression();
        }
    }

    @Nonnull
    private RelationalExpression handleSelectWithGroupBy(@Nonnull final RelationalParser.SelectElementsContext selectElements,
                                                         @Nonnull final RelationalParser.FromClauseContext fromClause,
                                                         @Nullable final RelationalParser.GroupByClauseContext groupByClause,
                                                         @Nullable final RelationalParser.HavingClauseContext havingClauseContext,
                                                         @Nullable final RelationalParser.OrderByClauseContext orderByClause,
                                                         @Nullable final RelationalParser.LimitClauseContext limitClause) {
        /*
         * planning GROUP BY is relatively complex, we do not end up generating a single QP node, but rather
         * three:
         *  - the underlying SELECT with quantifiers grouped separately, and an extra group for the grouping columns.
         *  - the GROUP BY expression with aggregate expression and grouping expressions.
         *  - an upper SELECT containing the final result set which are merely references to aggregate and grouping
         *    expressions in the underlying GROUP BY expression.
        */
        final var groupByQunAlias = CorrelationIdentifier.of(ParserUtils.toProtoBufCompliantName(CorrelationIdentifier.uniqueID().getId()));

        Quantifier underlyingSelectQun;
        // 1. create a new SELECT expression projecting all underlying quantifiers.
        {
            final var builder = GraphExpansion.builder();
            // handle underlying select.
            scopes.push();
            // grab all quantifiers into the current scope.
            fromClause.accept(this);
            final var scope = scopes.getCurrentScope();
            final var columns = scope.getAllQuantifiers().stream().map(qun -> {
                final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
                return Column.of(
                        Type.Record.Field.of(
                                quantifiedValue.getResultType(),
                                Optional.of(qun.getAlias().getId())),
                        quantifiedValue);
            });

            RecordConstructorValue groupingColumnsValue;
            // add everything in the group by clause to the result set.
            if (groupByClause != null) {
                groupByClause.accept(this);
                final var groupByColumns = scope.getProjectList();
                Assert.thatUnchecked(!groupByColumns.isEmpty());
                groupingColumnsValue = RecordConstructorValue.ofColumns(groupByColumns);
            } else {
                groupingColumnsValue = RecordConstructorValue.ofColumns(Collections.emptyList());
            }

            // the result set is { GB, q1, q2, q3, ... } | GB is the grouping-columns group, q1, q2, q3 are RCV of the underlying quantifiers.
            final List<Column<? extends Value>> resultColumns = Streams.concat(
                    Stream.of(Column.unnamedOf(groupingColumnsValue)),
                    columns).collect(Collectors.toList());
            builder.addAllQuantifiers(scope.getAllQuantifiers()).addAllResultColumns(resultColumns);
            if (scope.hasPredicate()) {
                builder.addPredicate(scope.getPredicate());
            }
            scopes.pop();
            underlyingSelectQun = Quantifier.forEach(GroupExpressionRef.of(builder.build().buildSelect()));
            scopes.push().addQuantifier(underlyingSelectQun); // for later processing
        }

        // 2. handle group by expression.
        Quantifier groupByQuantifier;
        Type groupByExpressionType;
        Optional<List<Column<? extends Value>>> orderByColumns = Optional.empty();
        {
            final var scope = scopes.getCurrentScope();
            scope.setFlag(Scopes.Scope.Flag.UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE); // set this flag, so we can resolve grouping identifiers correctly.
            // add aggregations (make a list of individual aggregations for the Agg expression) and other (group by and having) columns.
            selectElements.accept(this);
            if (havingClauseContext != null) {
                context.withDisabledLiteralProcessing(() -> havingClauseContext.accept(this));
            }
            // (yhatem) resolve order-by columns _now_ because we need them to build using the same resolution rules for group by
            if (orderByClause != null) {
                // currently this is not supported. (TODO)
                Assert.failUnchecked("order by clause in conjunction with group by is not currently supported", ErrorCode.UNSUPPORTED_OPERATION);
            }
            // (yhatem) it is possible to pull the aggregation values from the projection list e.g. via TreeLike.filter
            // but this is dangerous since we rely on two different visitation methods to give us back some children
            // (e.g. one does pre-order, the other does post-order => references are wrongly established in step 3).
            // therefore, we use Scope.getAggregateValues to give us back what we need, which, under the hood,
            // relies on Antler's visitation (again) giving us consistent results between here and step 3.
            final var aggregateValues = scope.getAggregateValues();
            aggregateValues.forEach(ParserUtils::verifyAggregateValue);
            final var aggregationValue = RecordConstructorValue.ofColumns(aggregateValues.stream().map(Column::unnamedOf).collect(Collectors.toList()));
            final var groupByExpression = new GroupByExpression(aggregationValue, groupByClause == null ?
                    null :
                    FieldValue.ofOrdinalNumber(underlyingSelectQun.getFlowedObjectValue(), 0), underlyingSelectQun);
            groupByExpressionType = groupByExpression.getResultValue().getResultType();
            final var groupByScope = scopes.pop();
            groupByQuantifier = Quantifier.forEach(GroupExpressionRef.of(groupByExpression), groupByQunAlias);
            final var selectHavingScope = scopes.push();
            selectHavingScope.addQuantifier(groupByQuantifier);
            selectHavingScope.addAllAggregateReferences(groupByScope.getAggregateReferences());
        }

        // 3. handle select on top of group by
        {
            final var scope = scopes.getCurrentScope();
            scope.setFlag(Scopes.Scope.Flag.UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE); // set this flag, so we can resolve grouping identifiers correctly.
            scope.setFlag(Scopes.Scope.Flag.RESOLVING_SELECT_HAVING); // set this flag, so we transform aggregate values into references.
            scope.setGroupByQuantifierCorrelation(groupByQunAlias);
            scope.setGroupByType(groupByExpressionType);
            scope.getProjectList().clear();
            visit(selectElements);
            if (havingClauseContext != null) {
                visit(havingClauseContext);
            }
            orderByColumns.ifPresent(columns -> columns.forEach(column -> scope.addOrderByColumn(column, false)));
            if (limitClause != null) {
                visit(limitClause);
            }
            return scopes.pop().convertToRelationalExpression();
        }
    }

    @Nonnull List<Column<? extends Value>> expandSelectWhereColumns(@Nonnull final Quantifier qun, @Nullable final Value id) {
        List<Column<? extends FieldValue>> expandedColumnNames;
        if (id == null) {
            expandedColumnNames = qun.getFlowedColumns().subList(1, qun.getFlowedColumns().size());
        } else {
            expandedColumnNames = qun.getFlowedColumns().stream()
                    .filter(column -> column.getValue().getFieldPathNames().get(0).equals(ParserUtils.toString(id)))
                    .collect(Collectors.toList());
        }
        // now validate each column using the parser to make sure that it is visible.
        final var fieldNames = expandedColumnNames.stream().flatMap(q -> {
            Assert.thatUnchecked(q.getValue().getResultType() instanceof Type.Record, "can not expand '*'");
            Type.Record record = (Type.Record) (q.getValue().getResultType());
            Assert.thatUnchecked(record.getFields().stream().noneMatch(f -> f.getFieldNameOptional().isEmpty()), "unable to expand '*' because of unnamed underlying columns");
            return record.getFields().stream().map(Type.Record.Field::getFieldName);
        }).collect(Collectors.toList());
        return fieldNames.stream().map(fieldName -> ParserUtils.resolveField(List.of(fieldName), scopes)).map(ParserUtils::toColumn).collect(Collectors.toList());
    }

    @Nonnull
    private List<Column<? extends Value>> expandStarColumns(Value id) {
        final var scope = scopes.getCurrentScope();
        final var isUnderlyingSelectWhere = scope.isFlagSet(Scopes.Scope.Flag.UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE);
        final var isUnderlyingGroupByExpression = scope.isFlagSet(Scopes.Scope.Flag.RESOLVING_SELECT_HAVING);
        /*
         * If we are expanding a star in a GROUP BY query, we have to be extra careful. In this case we will have
         * an underlying SELECT expression with a result set that looks like this: {GB, T1, T2, ...}, where GB
         * is the group by columns group, and T1, T2, ... are the result values of each underlying quantifier.
         *
         * We pick T1, T2, ... quantifiers, and expand their columns. The expanded columns are effectively the result
         * of star expansion, _however_ we still have to pass them to validation logic to make sure they are visible
         * with respect to the grouping columns.
         *
         * For example. Assume we have a table T1(id, col1, col2), a query like this should fail:
         *
         * SELECT * FROM T1 GROUP BY col1; -- expansion of '*' into T1's columns (id, col1, col2), this tuple does
         * not pass validation since id and col2 are not in the group by columns group.
         *
         * However, a query like this should succeed:
         *
         * SELECT * FROM (SELECT col1 from T1) AS X GROUP BY x.col1;
         */
        if (isUnderlyingGroupByExpression) { // drill down to select-where
            Assert.thatUnchecked(scope.getForEachQuantifiers().size() == 1);
            final var groupByQun = scopes.getCurrentScope().getForEachQuantifiers().get(0);
            final var selectWhereQun = groupByQun.getRangesOver().get().getQuantifiers().stream().filter(q -> q instanceof Quantifier.ForEach).collect(Collectors.toList());
            Assert.thatUnchecked(selectWhereQun.size() == 1);
            return expandSelectWhereColumns(selectWhereQun.get(0), id);

        } else if (isUnderlyingSelectWhere) { // expand { GB, -> {... rest} }
            Assert.thatUnchecked(scope.getForEachQuantifiers().size() == 1);
            final var qun = scopes.getCurrentScope().getForEachQuantifiers().get(0);
            return expandSelectWhereColumns(qun, id);
        } else {
            List<Column<? extends Value>> columns = new ArrayList<>();
            for (Quantifier quantifier : scope.getForEachQuantifiers()) {
                if (id == null || quantifier.getAlias().getId().equals(ParserUtils.toString(id))) {
                    for (var column : quantifier.getFlowedColumns()) {
                        var field = column.getField();
                        // create columns from field names leaving it to the constructor of fields to re-create their ordinal positions.
                        columns.add(Column.of(
                                Type.Record.Field.of(field.getFieldType(), field.getFieldNameOptional(), Optional.empty()),
                                column.getValue())
                        );
                    }
                }
            }
            return columns;
        }
    }

    @Override
    public Void visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx) {
        if (ctx.STAR() != null) {
            final var cols = expandStarColumns(null);
            final var scope = scopes.getCurrentScope();
            cols.forEach(scope::addProjectionColumn);
        } else {
            for (var selectElement : ctx.selectElement()) {
                visit(selectElement);
            }
        }
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Void visitSelectStarElement(RelationalParser.SelectStarElementContext ctx) {
        final var cols = expandStarColumns((Value) visit(ctx.uid()));
        final var scope = scopes.getCurrentScope();
        cols.forEach(scope::addProjectionColumn);
        return null;
    }

    @Override
    public Void visitSelectFunctionElement(RelationalParser.SelectFunctionElementContext ctx) {
        Column<Value> column;
        if (ctx.AS() != null) {
            column = ParserUtils.toColumn((Value) ctx.functionCall().accept(this),
                    Objects.requireNonNull(ParserUtils.safeCastLiteral(ctx.uid().accept(this), String.class)));
        } else {
            column = ParserUtils.toColumn((Value) ctx.functionCall().accept(this));
        }
        scopes.getCurrentScope().addProjectionColumn(column);
        return null;
    }

    @Override
    public Void visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext ctx) {
        final var expressionObj = ctx.expression().accept(this);
        Assert.thatUnchecked(expressionObj instanceof Value, UNSUPPORTED_QUERY);
        final var expression = (Value) expressionObj;
        Column<Value> column;
        if (ctx.AS() != null) {
            final String alias = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
            Assert.notNullUnchecked(alias, UNSUPPORTED_QUERY);
            column = ParserUtils.toColumn(expression, alias);
        } else {
            column = ParserUtils.toColumn(expression);
        }
        scopes.getCurrentScope().addProjectionColumn(column);
        return null;
    }

    @Override
    public Void visitOrderByClause(RelationalParser.OrderByClauseContext ctx) {
        ctx.orderByExpression().forEach(orderByExpression -> {
            var isReverse = (orderByExpression.ASC() == null) && (orderByExpression.DESC() != null);
            var column = ParserUtils.toColumn((Value) orderByExpression.expression().accept(this));
            scopes.getCurrentScope().addOrderByColumn(column, isReverse);
        });
        return null;
    }

    @Override
    public Void visitFromClause(RelationalParser.FromClauseContext ctx) {
        Assert.notNullUnchecked(ctx.FROM(), UNSUPPORTED_QUERY);
        // prepare parser context for resolving aliases by parsing FROM clause first.
        ctx.tableSources().accept(this);
        if (ctx.WHERE() != null) {
            final var predicate = ctx.whereExpr.accept(this);
            Assert.notNullUnchecked(predicate);
            Assert.thatUnchecked(predicate instanceof BooleanValue, String.format("unexpected predicate of type %s", predicate.getClass().getSimpleName()));
            final Collection<CorrelationIdentifier> aliases = scopes.getCurrentScope().getAllQuantifiers().stream().filter(qun -> qun instanceof Quantifier.ForEach).map(Quantifier::getAlias).collect(Collectors.toList()); // not sure this is correct
            Assert.thatUnchecked(!aliases.isEmpty());
            final var result = LiteralsUtils.toQueryPredicate((BooleanValue) predicate, aliases.stream().findFirst().get(), context);
            scopes.getCurrentScope().setPredicate(result);
        }
        return null;
    }

    @Override
    public Void visitGroupByClause(RelationalParser.GroupByClauseContext ctx) {
        final var scope = scopes.getCurrentScope();
        for (final var groupingCol : ctx.groupByItem()) {
            scope.addProjectionColumn((Column<? extends Value>) groupingCol.accept(this));
        }
        return null;
    }

    @Override
    public Object visitHavingClause(RelationalParser.HavingClauseContext ctx) {
        if (ctx.HAVING() != null) {
            final var predicateObj = ctx.havingExpr.accept(this);
            Assert.notNullUnchecked(predicateObj);
            Assert.thatUnchecked(predicateObj instanceof Value, UNSUPPORTED_QUERY);
            final Value predicateValue = (Value) predicateObj;
            Assert.thatUnchecked(predicateValue instanceof BooleanValue, String.format("unexpected predicate of type %s", predicateValue.getClass().getSimpleName()));
            final Collection<CorrelationIdentifier> aliases = scopes.getCurrentScope().getAllQuantifiers().stream().filter(qun -> qun instanceof Quantifier.ForEach).map(Quantifier::getAlias).collect(Collectors.toList()); // not sure this is correct
            Assert.thatUnchecked(!aliases.isEmpty());
            final var predicate = LiteralsUtils.toQueryPredicate((BooleanValue) predicateValue, aliases.stream().findFirst().get(), context);
            scopes.getCurrentScope().setPredicate(predicate);
        }
        return null;
    }

    @Override
    public Column<Value> visitGroupByItem(RelationalParser.GroupByItemContext ctx) {
        Assert.isNullUnchecked(ctx.order, UNSUPPORTED_QUERY);
        if (ctx.uid() != null) {
            return ParserUtils.toColumn((Value) ctx.expression().accept(this),
                    Objects.requireNonNull(ParserUtils.safeCastLiteral(ctx.uid().accept(this), String.class)));
        } else {
            return ParserUtils.toColumn((Value) ctx.expression().accept(this));
        }
    }

    @Override
    public Void visitLimitClause(RelationalParser.LimitClauseContext ctx) {
        var parentScope = scopes.getCurrentScope().getParent();
        var siblingScope = scopes.getCurrentScope().getSibling();
        Assert.thatUnchecked(parentScope == null && siblingScope == null,
                "LIMIT clause can only be specified with top-level SQL query.", ErrorCode.UNSUPPORTED_OPERATION);
        final var limit = (int) visit(ctx.limit);
        context.asDql().setLimit(limit);
        if (ctx.offset != null) {
            // Owing to TODO
            Assert.failUnchecked("OFFSET clause is not supported.");
        }
        // Owing to TODO
        if (scopes.getCurrentScope().getAllQuantifiers().size() > 1) {
            Assert.thatUnchecked(context.asDql().getLimit() == 0 && context.asDql().getOffset() == 0,
                    "LIMIT / OFFSET with multiple FROM elements is not supported.", ErrorCode.UNSUPPORTED_OPERATION);
        }
        return null;
    }

    @Override
    public Integer visitLimitClauseAtom(RelationalParser.LimitClauseAtomContext ctx) {
        // the child must be literal not a ConstantObjectValue because limit does not contribute anything
        // to the plan, it just controls the physical execution cursor.
        return context.withDisabledLiteralProcessing(() -> {
            final var literalValue = visitChildren(ctx);
            Assert.thatUnchecked(literalValue instanceof LiteralValue);
            final var limit = ((LiteralValue<?>) literalValue).getLiteralValue();
            Assert.thatUnchecked(limit instanceof Integer, "argument for LIMIT must be integer", ErrorCode.DATATYPE_MISMATCH);
            Assert.thatUnchecked((Integer) limit > 0, "LIMIT must be positive", ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE);
            return (Integer) limit;
        });
    }

    @Override
    public Void visitTableSources(RelationalParser.TableSourcesContext ctx) {
        Assert.thatUnchecked(ctx.tableSource().size() > 0, UNSUPPORTED_QUERY);
        ctx.tableSource().forEach(tableSource -> tableSource.accept(this));
        return null;
    }

    @Override
    public Void visitTableSourceBase(RelationalParser.TableSourceBaseContext ctx) {
        Assert.thatUnchecked(ctx.joinPart().isEmpty(), UNSUPPORTED_QUERY);
        ctx.tableSourceItem().accept(this);
        return null;
    }

    @Override
    public Void visitAtomTableItem(RelationalParser.AtomTableItemContext ctx) {
        Assert.isNullUnchecked(ctx.PARTITION(), UNSUPPORTED_QUERY);
        final Typed tableName = (Typed) ctx.tableName().accept(this);
        Assert.thatUnchecked(tableName instanceof QualifiedIdentifierValue);

        // get index hints
        final Set<String> allIndexes = context.asDql().getIndexNames();
        final Set<String> hintedIndexes = new HashSet<>();
        for (final RelationalParser.IndexHintContext indexHintContext : ctx.indexHint()) {
            hintedIndexes.addAll(visitIndexHint(indexHintContext));
        }
        // check if all hinted indexes exist
        Assert.thatUnchecked(
                Sets.difference(hintedIndexes, allIndexes).isEmpty(),
                String.format("Unknown index(es) %s", String.join(",", Sets.difference(hintedIndexes, allIndexes))),
                ErrorCode.UNDEFINED_INDEX);
        Set<AccessHint> accessHintSet = hintedIndexes.stream().map(IndexAccessHint::new).collect(Collectors.toSet());

        final RelationalExpression from = ParserUtils.quantifyOver((QualifiedIdentifierValue) tableName, context, scopes,
                new AccessHints(accessHintSet.toArray(AccessHint[]::new)));
        final var quantifierAlias = ctx.alias != null ?
                ParserUtils.safeCastLiteral(visit(ctx.alias), String.class) :
                ParserUtils.safeCastLiteral(tableName, String.class);
        final CorrelationIdentifier aliasId = CorrelationIdentifier.of(quantifierAlias);
        final Quantifier.ForEach forEachQuantifier = Quantifier.forEachBuilder().withAlias(aliasId).build(GroupExpressionRef.of(from));
        scopes.getCurrentScope().addQuantifier(forEachQuantifier);
        return null;
    }

    @Override
    public Set<String> visitIndexHint(RelationalParser.IndexHintContext ctx) {
        // currently only support USE INDEX '(' uidList ')' syntax
        Assert.isNullUnchecked(ctx.IGNORE(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.FORCE(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.KEY(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.FOR(), UNSUPPORTED_QUERY);

        return ctx.uidList().uid().stream().map(this::visit).map(f -> ParserUtils.safeCastLiteral(f, String.class)).collect(Collectors.toSet());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Typed visitSubqueryTableItem(RelationalParser.SubqueryTableItemContext ctx) {
        Assert.notNullUnchecked(ctx.alias);
        final var subqueryAlias = ParserUtils.safeCastLiteral(ctx.alias.accept(this), String.class);
        Assert.notNullUnchecked(subqueryAlias);
        final var relationalExpression = ctx.selectStatement() != null ?
                ctx.selectStatement().accept(this) :
                ctx.parenthesisSubquery.accept(this);
        Assert.thatUnchecked(relationalExpression instanceof RelationalExpression);
        final RelationalExpression from = (RelationalExpression) relationalExpression;
        final Quantifier.ForEach forEachQuantifier = Quantifier.forEachBuilder().withAlias(CorrelationIdentifier.of(subqueryAlias)).build(GroupExpressionRef.of(from));
        //        if(ParserUtils.requiresCanonicalSubSelect(forEachQuantifier, parserContext)) {
        //            final var nestedForEachQuantifier = Quantifier.forEachBuilder().withAlias(CorrelationIdentifier.of(subqueryAlias + "__internal")).build(GroupExpressionRef.of(from));
        //            Quantifier.ForEach quantifier = Quantifier.forEachBuilder().withAlias(CorrelationIdentifier.of(subqueryAlias))
        //                    .build(GroupExpressionRef.of(GraphExpansion.ofQuantifier(nestedForEachQuantifier).buildSimpleSelectOverQuantifier(nestedForEachQuantifier)));
        //            parserContext.getCurrentScope().addQuantifier(quantifier);
        //        } else {
        //            parserContext.getCurrentScope().addQuantifier(forEachQuantifier);
        //        }
        scopes.getCurrentScope().addQuantifier(forEachQuantifier);
        return null;
    }

    //// Expressions /////

    @Override
    public Value visitNotExpression(RelationalParser.NotExpressionContext ctx) {
        return new NotValue((Value) visit(ctx.expression()));
    }

    @Override
    public Value visitLogicalExpression(RelationalParser.LogicalExpressionContext ctx) {
        Assert.notNullUnchecked(ctx.logicalOperator(), UNSUPPORTED_QUERY);
        Assert.thatUnchecked(ctx.logicalOperator().OR() != null || ctx.logicalOperator().AND() != null,
                String.format("logical operator %s is not supported", ctx.logicalOperator().getText()));
        final Value left = (Value) (visit(ctx.expression(0)));
        final Value right = (Value) (visit(ctx.expression(1)));

        if (ctx.logicalOperator().AND() != null) {
            return (Value) ParserUtils.encapsulate(AndOrValue.AndFn.class, List.of(left, right));
        }
        return (Value) ParserUtils.encapsulate(AndOrValue.OrFn.class, List.of(left, right));
    }

    @Override
    public Value visitIsExpression(RelationalParser.IsExpressionContext ctx) {
        final Value left = (Value) (visit(ctx.predicate()));
        if (ctx.NULL_LITERAL() != null) {
            if (ctx.NOT() != null) {
                return (Value) ParserUtils.encapsulate(RelOpValue.NotNullFn.class, List.of(left));
            } else {
                return (Value) ParserUtils.encapsulate(RelOpValue.IsNullFn.class, List.of(left));
            }
        } else {
            LiteralValue<Boolean> right = (ctx.TRUE() != null) ?
                    new LiteralValue<>(true) :
                    new LiteralValue<>(false);
            final Typed nullClause;
            Class<? extends BuiltInFunction<Value>> combineFunc;
            if (ctx.NOT() != null) {
                //invert the condition, and add an allowance for null as well -- e.g. is not true => (is false or is null)
                right = new LiteralValue<>(right.getResultType(), Boolean.FALSE.equals(right.getLiteralValue()));
                nullClause = ParserUtils.encapsulate(RelOpValue.IsNullFn.class, List.of(left));
                combineFunc = AndOrValue.OrFn.class;
            } else {
                nullClause = ParserUtils.encapsulate(RelOpValue.NotNullFn.class, List.of(left));
                combineFunc = AndOrValue.AndFn.class;
            }
            final Typed equals = ParserUtils.encapsulate(RelOpValue.EqualsFn.class, List.of(left, right));
            return (Value) ParserUtils.encapsulate(combineFunc, List.of(nullClause, equals));
        }
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Value visitLikePredicate(RelationalParser.LikePredicateContext ctx) {
        Assert.thatUnchecked(ctx.predicate().size() == 2);
        String escapeChar = null;
        if (ctx.STRING_LITERAL() != null) {
            escapeChar = ParserUtils.normalizeString(ctx.STRING_LITERAL().getText());
            Assert.thatUnchecked(escapeChar != null);
            Assert.thatUnchecked(escapeChar.length() == 1);
        }
        final var likeFn = new LikeOperatorValue.LikeFn();
        final var patternFn = new PatternForLikeValue.PatternForLikeFn();
        var result = (Value) ParserUtils.encapsulate(
                likeFn,
                List.of(
                        (Value) visit(ctx.predicate(0)),
                        (Value) ParserUtils.encapsulate(
                                patternFn,
                                List.of(
                                        (Value) visit(ctx.predicate(1)),
                                        new LiteralValue<>(escapeChar))
                        )));
        if (ctx.NOT() != null) {
            result = new NotValue(result);
        }
        return result;
    }

    ///// Predicates ///////

    @Override
    public Value visitInPredicate(RelationalParser.InPredicateContext ctx) {
        if (ctx.NOT() != null) {
            Assert.failUnchecked("NOT IN is not supported", ErrorCode.SYNTAX_ERROR);
        }
        if (ctx.inList().selectStatement() != null) {
            Assert.failUnchecked("IN <SELECT_STATEMENT> is not supported", ErrorCode.SYNTAX_ERROR);
        }

        Typed typedList;
        if (ctx.inList().preparedStatementParameter() != null) {
            typedList = (Typed) visit(ctx.inList().preparedStatementParameter());
        } else {
            if (ParserUtils.isConstant(ctx.inList().expressions())) {
                final int index = context.startArrayLiteral();
                final List<Value> values = new ArrayList<>();
                ctx.inList().expressions().expression().forEach(exp -> values.add((Value) visit(exp)));
                context.finishArrayLiteral();
                ParserUtils.validateInValuesList(values);
                typedList = LiteralsUtils.processArrayLiteral(values, index, context);
            } else {
                final List<Value> values = new ArrayList<>();
                ctx.inList().expressions().expression().forEach(exp -> values.add((Value) visit(exp)));
                typedList = ParserUtils.encapsulate(AbstractArrayConstructorValue.ArrayFn.class, ParserUtils.validateInValuesList(values));
            }
        }
        final var left = (Value) visit(ctx.predicate());
        return (Value) ParserUtils.encapsulate(InOpValue.InFn.class, List.of(left, typedList));
    }

    @Override
    public Value visitBinaryComparisonPredicate(RelationalParser.BinaryComparisonPredicateContext ctx) {
        final Value left = (Value) (visit(ctx.left));
        final Value right = (Value) (visit(ctx.right));
        BuiltInFunction<? extends Value> comparisonFunction = ParserUtils.getExplicitFunction(ctx.comparisonOperator().getText());
        return (Value) ParserUtils.encapsulate(comparisonFunction, List.of(left, right));
    }

    ///// Expression Atoms //////

    @Override
    public Value visitFunctionCallExpressionAtom(RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return (Value) visitChildren(ctx);
    }

    @Override
    public Typed visitPreparedStatementParameter(RelationalParser.PreparedStatementParameterContext ctx) {
        Object param;
        if (ctx.QUESTION() != null) {
            param = context.getPreparedStatementParameters().getNextParameter();
        } else {
            param = context.getPreparedStatementParameters().getNamedParameter(ctx.NAMED_PARAMETER().getText().substring(1));
        }
        if (param instanceof Array) {
            try {
                final int index = context.startArrayLiteral();
                final List<Value> values = new ArrayList<>();
                try (ResultSet rs = ((Array) param).getResultSet()) {
                    while (rs.next()) {
                        final var arrayParam = rs.getObject(1);
                        Value literal = new LiteralValue<>(arrayParam);
                        LiteralsUtils.processLiteral(literal, arrayParam, context);
                        values.add(literal);
                    }
                }
                context.finishArrayLiteral();
                //ParserUtils.validateInValuesList(values);
                return LiteralsUtils.processArrayLiteral(values, index, context);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }  else {
            final var literal = new LiteralValue<>(param);
            return LiteralsUtils.processLiteral(literal, param, context);
        }
    }

    @Override
    public Value visitExistsExpressionAtom(RelationalParser.ExistsExpressionAtomContext ctx) {
        Assert.notNullUnchecked(ctx.selectStatement());
        scopes.push();
        final var expression = ctx.selectStatement().accept(this);
        scopes.pop();
        Assert.thatUnchecked(expression instanceof RelationalExpression);
        final RelationalExpression nestedSubquery = (RelationalExpression) expression;
        Assert.notNullUnchecked(nestedSubquery);
        final Quantifier.Existential existsQuantifier = Quantifier.existential(GroupExpressionRef.of(nestedSubquery));
        scopes.getCurrentScope().addQuantifier(existsQuantifier);
        return new ExistsValue(existsQuantifier.getFlowedObjectValue());
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitSubqueryExpressionAtom(RelationalParser.SubqueryExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitMathExpressionAtom(RelationalParser.MathExpressionAtomContext ctx) {
        final Value left = (Value) (visit(ctx.left));
        final Value right = (Value) (visit(ctx.right));
        BuiltInFunction<? extends Value> mathFunction = ParserUtils.getExplicitFunction(ctx.mathOperator().getText());
        return (Value) ParserUtils.encapsulate(mathFunction, List.of(left, right));
    }

    ////// DB Objects /////


    @Override
    public Value visitFullId(RelationalParser.FullIdContext ctx) {
        Assert.thatUnchecked(!ctx.uid().isEmpty());
        Assert.thatUnchecked(ctx.uid().size() > 0);
        return QualifiedIdentifierValue.of(ctx.uid().stream().map(this::visit).map(f -> ParserUtils.safeCastLiteral(f, String.class)).map(Assert::notNullUnchecked).toArray(String[]::new));
    }

    @Override
    public Value visitTableName(RelationalParser.TableNameContext ctx) {
        final Typed fullId = (Typed) ctx.fullId().accept(this);
        Assert.thatUnchecked(fullId instanceof QualifiedIdentifierValue);
        final QualifiedIdentifierValue qualifiedIdentifierValue = (QualifiedIdentifierValue) fullId;
        Assert.thatUnchecked(qualifiedIdentifierValue.getParts().length <= 2);
        return qualifiedIdentifierValue;
    }

    @Override
    public Value visitFullColumnNameExpressionAtom(RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        return (Value) (ctx.fullColumnName().accept(this));
    }

    @Override
    public Value visitFullColumnName(RelationalParser.FullColumnNameContext ctx) {
        final var id = ctx.fullId().accept(this);
        Assert.thatUnchecked(id instanceof QualifiedIdentifierValue);
        final QualifiedIdentifierValue qualifiedIdentifierValue = (QualifiedIdentifierValue) id;
        final var fieldParts = Arrays.stream(qualifiedIdentifierValue.getParts()).collect(Collectors.toList());
        return ParserUtils.resolveField(fieldParts, scopes);
        //        final var qunInfo = ParserUtils.findFieldPath(fieldParts.get(0), parserContext);
        //        if (qunInfo.getLeft().getAlias().toString().equals(fieldParts.get(0))) {
        //            fieldParts = fieldParts.stream().skip(1).collect(Collectors.toList());
        //        }
        //        qunInfo.getRight().addAll(fieldParts);
        //        final FieldValue fieldValue = ParserUtils.getFieldValue(qunInfo.getRight(), qunInfo.getLeft().getFlowedObjectValue());
        //        return fieldValue;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitIndexColumnName(RelationalParser.IndexColumnNameContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitCharsetName(RelationalParser.CharsetNameContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitCollationName(RelationalParser.CollationNameContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitUid(RelationalParser.UidContext ctx) {
        if (ctx.simpleId() != null) {
            return LiteralValue.ofScalar(ParserUtils.safeCastLiteral(visit(ctx.simpleId()), String.class));
        } else {
            return LiteralValue.ofScalar(ParserUtils.normalizeString(ctx.getText()));
        }
    }

    @Override
    public Value visitSimpleId(RelationalParser.SimpleIdContext ctx) {
        return LiteralValue.ofScalar(ParserUtils.normalizeString(ctx.getText()));
    }

    @Override
    public Object visitRecordConstructorForInsert(RelationalParser.RecordConstructorForInsertContext ctx) {
        final var columns = visitRecordFieldContextsUnderReorderings(ctx.expressionWithOptionalName());
        return RecordConstructorValue.ofColumns(columns);
    }

    @Override
    public Object visitRecordConstructor(RelationalParser.RecordConstructorContext ctx) {
        List<Column<? extends Value>> columns;
        if (ctx.expressionWithName() != null) {
            columns = visitRecordFieldContextsUnderReorderings(ImmutableList.of(ctx.expressionWithName()));
        } else {
            columns = visitRecordFieldContextsUnderReorderings(ctx.expressionWithOptionalName());
        }
        return RecordConstructorValue.ofColumns(columns);
    }

    @Nonnull
    private static Column<? extends Value> coerceUndefinedTypes(@Nonnull final Column<? extends Value> column, @Nonnull final Type targetType) {
        final Value value = column.getValue();
        final var resultType = value.getResultType();
        if (resultType.isUnresolved()) {
            Preconditions.checkArgument(value.canBePromotedToType(targetType));
            return Column.of(Type.Record.Field.of(targetType, column.getField().getFieldNameOptional()), value.promoteToType(targetType));
        }
        return column;
    }

    private List<Column<? extends Value>> visitRecordFieldContextsUnderReorderings(@Nonnull final List<? extends ParserRuleContext> providedColumnContexts) {
        if (context.isDql() || (context.isDml() && !context.asDml().hasTargetType())) {
            return visitRecordFieldContexts(providedColumnContexts, null);
        }

        final var currentScope = context.asDml();
        final var targetType = (Type.Record) currentScope.getTargetType();
        final var elementFields = Assert.notNullUnchecked(targetType.getFields());

        if (currentScope.hasTargetTypeReorderings()) {
            final var targetTypeReorderings = ImmutableList.copyOf(Assert.notNullUnchecked(currentScope.getTargetTypeReorderings().getChildrenMap()).keySet());
            final var resultColumnsBuilder = ImmutableList.<Column<? extends Value>>builder();

            for (final var elementField : elementFields) {
                final int index = targetTypeReorderings.indexOf(elementField.getFieldName());
                final var fieldType = elementField.getFieldType();
                final Column<? extends Value> currentFieldColumns;
                if (index >= 0) {
                    currentFieldColumns = visitRecordFieldContext(providedColumnContexts.get(index), elementField);
                } else {
                    currentFieldColumns = Column.unnamedOf(ParserUtils.resolveDefaultValue(fieldType));
                }
                resultColumnsBuilder.add(currentFieldColumns);
            }
            return resultColumnsBuilder.build();
        }

        Assert.thatUnchecked(elementFields.size() == providedColumnContexts.size(),
                "provided record cannot be assigned as its type is incompatible with the target type",
                ErrorCode.CANNOT_CONVERT_TYPE);
        return visitRecordFieldContexts(providedColumnContexts, elementFields);
    }

    @Nonnull
    private List<Column<? extends Value>> visitRecordFieldContexts(@Nonnull final List<? extends ParserRuleContext> parserRuleContexts,
                                                                   @Nullable final List<Type.Record.Field> targetFields) {
        Assert.thatUnchecked(targetFields == null || targetFields.size() == parserRuleContexts.size());
        final var resultsBuilder = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < parserRuleContexts.size(); i++) {
            final var parserRuleContext = parserRuleContexts.get(i);
            final var targetField = targetFields == null ? null : targetFields.get(i);
            resultsBuilder.add(visitRecordFieldContext(parserRuleContext, targetField));
        }
        return resultsBuilder.build();
    }

    @Nonnull
    private Column<? extends Value> visitRecordFieldContext(@Nonnull final ParserRuleContext parserRuleContext,
                                                            @Nullable final Type.Record.Field targetField) {
        final var fieldType = targetField == null ? null : targetField.getFieldType();
        final StringTrieNode reorderings;
        if (targetField != null && context.isDml()) {
            final var dmlContext = context.asDml();
            if (dmlContext.hasTargetTypeReorderings()) {
                reorderings = dmlContext.getTargetTypeReorderings();
            } else {
                reorderings = null;
            }
        } else {
            reorderings = null;
        }
        final var targetFieldReorderings = (reorderings == null || reorderings.getChildrenMap() == null) ? null : reorderings.getChildrenMap().get(targetField.getFieldName());
        final var nestedDmlContext = context.pushDmlContext();
        final Column<? extends Value> column;
        try {
            if (fieldType != null) {
                nestedDmlContext.setTargetType(fieldType);
            }
            if (targetFieldReorderings != null && targetFieldReorderings.getChildrenMap() != null) {
                nestedDmlContext.setTargetTypeReorderings(targetFieldReorderings);
            }
            column = (Column<? extends Value>) parserRuleContext.accept(this);
        } finally {
            context.pop();
        }
        Assert.notNullUnchecked(column);
        if (fieldType == null) {
            return column;
        }
        return coerceUndefinedTypes(column, fieldType);
    }

    @Override
    public Object visitExpressionsWithDefaults(RelationalParser.ExpressionsWithDefaultsContext ctx) {
        final var columnsBuilder = ImmutableList.<Column<? extends Value>>builder();
        for (final var expressionOrDefault : ctx.expressionOrDefault()) {
            columnsBuilder.add(Column.unnamedOf((Value) visit(expressionOrDefault)));
        }
        return RecordConstructorValue.ofColumns(columnsBuilder.build());
    }

    @Override
    public Object visitExpressionOrDefault(RelationalParser.ExpressionOrDefaultContext ctx) {
        Assert.isNullUnchecked(ctx.DEFAULT(), UNSUPPORTED_QUERY);
        return visit(ctx.expression());
    }

    @Override
    public Object visitExpressionWithName(RelationalParser.ExpressionWithNameContext ctx) {
        final var nestedValue = (Value) visit(ctx.expression());
        final var name = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(ctx.uid(), String.class));
        return Column.of(Type.Record.Field.of(nestedValue.getResultType(), Optional.of(name)), nestedValue);
    }

    @Override
    public Object visitExpressionWithOptionalName(RelationalParser.ExpressionWithOptionalNameContext ctx) {
        final var nestedValue = (Value) visit(ctx.expression());
        if (ctx.AS() != null) {
            final var name = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class));
            return Column.of(Type.Record.Field.of(nestedValue.getResultType(), Optional.of(name)), nestedValue);
        } else {
            return Column.unnamedOf(nestedValue);
        }
    }

    /////// Literals //////

    @Override
    public Value visitBooleanLiteral(RelationalParser.BooleanLiteralContext ctx) {
        if (ctx.FALSE() != null) {
            final var literal = new LiteralValue<>(false);
            return LiteralsUtils.processLiteral(literal, literal.getLiteralValue(), context);
        } else {
            Assert.notNullUnchecked(ctx.TRUE(), String.format("unexpected boolean value %s", ctx.getText()));
            final var literal = new LiteralValue<>(true);
            return LiteralsUtils.processLiteral(literal, literal.getLiteralValue(), context);
        }
    }

    @Override
    public Value visitHexadecimalLiteral(RelationalParser.HexadecimalLiteralContext ctx) {
        Assert.isNullUnchecked(ctx.STRING_CHARSET_NAME(), UNSUPPORTED_QUERY);
        Assert.notNullUnchecked(ctx.HEXADECIMAL_LITERAL(), UNSUPPORTED_QUERY);
        // todo (yhatem) test this.
        final var literal = new LiteralValue<>(new BigInteger(ctx.HEXADECIMAL_LITERAL().getText().substring(2), 16).longValue());
        return LiteralsUtils.processLiteral(literal, literal.getLiteralValue(), context);
    }

    @Override
    public Value visitStringLiteral(RelationalParser.StringLiteralContext ctx) {
        Assert.isNullUnchecked(ctx.STRING_CHARSET_NAME(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.START_NATIONAL_STRING_LITERAL(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.COLLATE(), UNSUPPORTED_QUERY);
        final var literal = new LiteralValue<>(ParserUtils.normalizeString(ctx.getText()));
        return LiteralsUtils.processLiteral(literal, literal.getLiteralValue(), context);
    }

    @Override
    public Value visitDecimalLiteral(RelationalParser.DecimalLiteralContext ctx) {
        final var literal = ParserUtils.parseDecimal(ctx.getText());
        return LiteralsUtils.processLiteral(literal, literal.getLiteralValue(), context);
    }

    @Override
    public Value visitNegativeDecimalConstant(RelationalParser.NegativeDecimalConstantContext ctx) {
        final var literal = ParserUtils.parseDecimal(ctx.getText());
        return LiteralsUtils.processLiteral(literal, literal.getLiteralValue(), context);
    }

    @Override
    public Value visitNullConstant(RelationalParser.NullConstantContext ctx) {
        Assert.isNullUnchecked(ctx.NOT(), UNSUPPORTED_QUERY);
        return (Value) (visit(ctx.nullLiteral()));
    }

    @Override
    public Value visitNullLiteral(RelationalParser.NullLiteralContext ctx) {
        return new NullValue(Type.nullType()); // do not strip nulls.
    }

    /////// Lists //////


    @Override
    public StringTrieNode visitUidListWithNestingsInParens(RelationalParser.UidListWithNestingsInParensContext ctx) {
        return visitUidListWithNestings(ctx.uidListWithNestings());
    }

    @Override
    public StringTrieNode visitUidListWithNestings(RelationalParser.UidListWithNestingsContext ctx) {
        final var uidMap =
                ctx.uidWithNestings()
                        .stream()
                        .map(this::visitUidWithNestings)
                        .collect(ImmutableMap.toImmutableMap(pair -> Assert.notNullUnchecked(pair).getLeft(),
                                pair -> Assert.notNullUnchecked(pair).getRight(),
                                (l, r) -> {
                                    throw Assert.failUnchecked("duplicate column", ErrorCode.AMBIGUOUS_COLUMN);
                                }));
        return new StringTrieNode(uidMap);
    }

    @Override
    @Nonnull
    public Pair<String, StringTrieNode> visitUidWithNestings(RelationalParser.UidWithNestingsContext ctx) {
        final var uid = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class));
        if (ctx.uidListWithNestingsInParens() == null) {
            return Pair.of(uid, StringTrieNode.leafNode());
        } else {
            return Pair.of(uid, visitUidListWithNestingsInParens(ctx.uidListWithNestingsInParens()));
        }
    }

    /////// Functions ////////////////////


    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Object visitSpecificFunctionCall(RelationalParser.SpecificFunctionCallContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitAggregateFunctionCall(RelationalParser.AggregateFunctionCallContext ctx) {
        return (Value) ctx.aggregateWindowedFunction().accept(this);
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Object visitNonAggregateFunctionCall(RelationalParser.NonAggregateFunctionCallContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Object visitScalarFunctionCall(RelationalParser.ScalarFunctionCallContext ctx) {
        final List<Typed> args = ctx.functionArgs().children.stream()
                .filter(arg -> arg instanceof RelationalParser.FunctionArgContext)
                .map(arg -> (Typed) arg.accept(this))
                .collect(Collectors.toList());
        BuiltInFunction<? extends Value> scalarFunction = ParserUtils.getExplicitFunction(ctx.scalarFunctionName().getText());
        return (Value) ParserUtils.encapsulate(scalarFunction, args);
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Object visitUdfFunctionCall(RelationalParser.UdfFunctionCallContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitAggregateWindowedFunction(RelationalParser.AggregateWindowedFunctionContext ctx) {
        Assert.isNullUnchecked(ctx.BIT_AND(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.BIT_OR(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.BIT_XOR(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.STD(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.STDDEV(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.STDDEV_POP(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.STDDEV_SAMP(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.VAR_POP(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.VAR_SAMP(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.VARIANCE(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.GROUP_CONCAT(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.overClause(), UNSUPPORTED_QUERY);

        if (ctx.aggregator != null) {
            Assert.thatUnchecked(ctx.aggregator.getText().equals(ctx.ALL().getText()), UNSUPPORTED_QUERY);
        }

        final var scope = scopes.getCurrentScope();
        if (scope.isFlagSet(Scopes.Scope.Flag.RESOLVING_SELECT_HAVING)) {
            final var groupByQuantifier = QuantifiedObjectValue.of(Objects.requireNonNull(scope.getGroupByQuantifierCorrelation()), Objects.requireNonNull(scope.getGroupByType()));
            final var aggExprSelector = FieldValue.ofOrdinalNumber(groupByQuantifier, ParserUtils.getLastFieldIndex(groupByQuantifier.getResultType()));
            final var aggregationReference = FieldValue.ofOrdinalNumber(aggExprSelector, scope.getAggregateReferences().get(scope.getAggregateCounter()));
            scope.increaseAggCounter();
            return aggregationReference;
        } else {
            final var functionName = ctx.functionName.getText().toUpperCase(Locale.ROOT);
            final AggregateValue aggregateValue;
            if (ctx.starArg != null) {
                aggregateValue = (AggregateValue) ParserUtils.resolveAndEncapsulate(functionName, List.of(RecordConstructorValue.ofColumns(List.of())));
            } else if (ctx.functionArg() != null) {
                scope.setFlag(Scopes.Scope.Flag.RESOLVING_AGGREGATION);
                final var argument = ctx.functionArg().accept(this);
                scope.unsetFlag(Scopes.Scope.Flag.RESOLVING_AGGREGATION);
                aggregateValue = (AggregateValue) ParserUtils.resolveAndEncapsulate(functionName, List.of((Value) argument));
            } else {
                aggregateValue = (AggregateValue) ParserUtils.resolveAndEncapsulate(functionName, List.of());
            }
            scope.addAggregateValue(aggregateValue);
            return aggregateValue;
        }
    }

    /////// DDL statements //////////////

    @Override
    public ProceduralPlan visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        context.pushDdlContext();
        Assert.thatUnchecked(ctx.createStatement() != null || ctx.dropStatement() != null, UNSUPPORTED_QUERY);
        final var result = (ProceduralPlan) visitChildren(ctx);
        context.pop();
        return result;
    }

    @Override
    public ProceduralPlan visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx) {
        final String schemaId = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.schemaId()), String.class));
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(schemaId);
        final String templateId = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.schemaTemplateId()), String.class));
        return ProceduralPlan.of(context.asDdl().getMetadataOperationsFactory().getCreateSchemaConstantAction(dbAndSchema.getLeft().orElse(dbUri),
                dbAndSchema.getRight(), templateId, Options.NONE));
    }

    @Override
    public Void visitTemplateClause(RelationalParser.TemplateClauseContext ctx) {
        if (ctx.structOrTableDefinition() != null) {
            ctx.structOrTableDefinition().accept(this);
        } else if (ctx.enumDefinition() != null) {
            ctx.enumDefinition().accept(this);
        } else {
            visit(ctx.indexDefinition());
        }
        return null;
    }

    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        final var schemaTemplateName = ParserUtils.safeCastLiteral(visit(ctx.schemaTemplateId()), String.class);
        // schema template version will be set automatically at update operation to lastVersion + 1
        final var schemaTemplateBuilder = context.asDdl().getMetadataBuilder().setName(schemaTemplateName).setVersion(1);
        if (ctx.optionsClause() != null) {
            for (var option : ctx.optionsClause().option()) {
                if (option.ENABLE_LONG_ROWS() != null) {
                    schemaTemplateBuilder.setEnableLongRows(option.booleanLiteral().TRUE() != null);
                } else if (option.INTERMINGLE_TABLES() != null) {
                    schemaTemplateBuilder.setIntermingleTables(option.booleanLiteral().TRUE() != null);
                } else {
                    Assert.failUnchecked("Encountered unknown options in schema template creation", ErrorCode.SYNTAX_ERROR);
                }
            }
        }
        // collect all tables, their indices, and custom types definitions.
        ctx.templateClause().forEach(s -> s.accept(this));
        return ProceduralPlan.of(context.asDdl().getMetadataOperationsFactory().getCreateSchemaTemplateConstantAction(schemaTemplateBuilder.build(), Options.NONE));
    }

    @Override
    public ProceduralPlan visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
        final String dbName = ParserUtils.safeCastLiteral(visit(ctx.path()), String.class);
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        return ProceduralPlan.of(context.asDdl().getMetadataOperationsFactory().getCreateDatabaseConstantAction(URI.create(dbName), Options.NONE));
    }

    @Override
    public Void visitStructOrTableDefinition(RelationalParser.StructOrTableDefinitionContext ctx) {
        Assert.thatUnchecked(ctx.STRUCT() == null || ctx.primaryKeyDefinition() == null,
                String.format("Illegal struct definition '%s'", ctx.uid().getText()), ErrorCode.SYNTAX_ERROR);
        Assert.thatUnchecked(ctx.TABLE() == null || ctx.primaryKeyDefinition() != null,
                String.format("Illegal table definition '%s'. Include either a PRIMARY KEY clause OR A SINGLE ROW ONLY clause.", ctx.uid().getText()), ErrorCode.SYNTAX_ERROR);
        final var name = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(ctx.uid().accept(this), String.class));
        final var columns = ctx.columnDefinition().stream().map(c -> (RecordLayerColumn) c.accept(this)).collect(Collectors.toList());
        final var isTable = ctx.STRUCT() == null;

        final var typeBuilder = RecordLayerTable.newBuilder(context.asDdl().getMetadataBuilder().isIntermingleTables());
        typeBuilder.setName(name).addColumns(columns);

        if (ctx.primaryKeyDefinition() != null) {
            final var primaryKeyParts = (List<List<String>>) ctx.primaryKeyDefinition().accept(this);
            primaryKeyParts.forEach(typeBuilder::addPrimaryKeyPart);
        }
        if (isTable) {
            context.asDdl().getMetadataBuilder().addTable(typeBuilder.build());
        } else {
            context.asDdl().getMetadataBuilder().addAuxiliaryType(typeBuilder.build().getDatatype());
        }
        return null;
    }

    @Override
    public RecordLayerColumn visitColumnDefinition(RelationalParser.ColumnDefinitionContext ctx) {
        final String columnName = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.colName), String.class));

        boolean isNullable = true;
        if (ctx.columnConstraint() != null) {
            isNullable = (Boolean) ctx.columnConstraint().accept(this);
            if (!isNullable) {
                containsNonNullableArray = true;
            }
        }

        boolean isRepeated = ctx.ARRAY() != null;

        var dataType = visitColumnType(ctx.columnType());
        if (isRepeated) {
            dataType = DataType.ArrayType.from(dataType, isNullable);
        } else {
            dataType = dataType.withNullable(isNullable);
        }

        return RecordLayerColumn.newBuilder().setName(columnName).setDataType(dataType).build();
    }

    @Override
    public DataType visitColumnType(RelationalParser.ColumnTypeContext ctx) {
        if (ctx.customType != null) {
            final var typeName = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.customType), String.class));
            return ParserUtils.toRelationalType(typeName, false, false, context.asDdl().getMetadataBuilder());
        } else {
            return visitPrimitiveType(ctx.primitiveType());
        }
    }

    @Override
    public DataType visitPrimitiveType(RelationalParser.PrimitiveTypeContext ctx) {
        String primitiveTypeName = ctx.getText();
        return ParserUtils.toRelationalType(primitiveTypeName, false, false, null);
    }

    @Override
    public Boolean visitNullColumnConstraint(RelationalParser.NullColumnConstraintContext ctx) {
        return ctx.nullNotnull().NOT() == null;
    }

    @Override
    public List<List<String>> visitPrimaryKeyDefinition(RelationalParser.PrimaryKeyDefinitionContext ctx) {
        if (ctx.fullId().size() == 0) {
            return List.of();
        }
        return ctx.fullId().stream().map(this::visit).map(f -> ((QualifiedIdentifierValue) (f)).getParts()).map(Arrays::asList).collect(Collectors.toList());
    }

    @Override
    public Void visitEnumDefinition(RelationalParser.EnumDefinitionContext ctx) {
        final var name = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class));
        // (yhatem) we have control over the ENUM values' numbers.
        final List<DataType.EnumType.EnumValue> enumValues = new ArrayList<>(ctx.STRING_LITERAL().size());
        for (int i = 0; i < ctx.STRING_LITERAL().size(); i++) {
            enumValues.add(DataType.EnumType.EnumValue.of(Assert.notNullUnchecked(ParserUtils.normalizeString(ctx.STRING_LITERAL(i).getText())), i));
        }
        final var enumType = DataType.EnumType.from(name, enumValues, false); // (yhatem) we should make it possible to have nullable enums maybe?
        context.asDdl().getMetadataBuilder().addAuxiliaryType(enumType);
        return null;
    }

    @Override
    public Void visitIndexDefinition(RelationalParser.IndexDefinitionContext ctx) {
        final String indexName = ParserUtils.safeCastLiteral(ctx.indexName.accept(this), String.class);
        Assert.notNullUnchecked(indexName);

        // parse the index query using the newly-constructed metadata so far
        final var schemaTemplate = context.asDdl().getMetadataBuilder().build();
        context.pushDqlContext(schemaTemplate);
        final var viewPlan = (RelationalExpression) ctx.querySpecification().accept(this);
        context.pop();

        final var isUnique = ctx.UNIQUE() != null;
        final var generator = IndexGenerator.from(viewPlan);
        final var table = context.asDdl().getMetadataBuilder().extractTable(generator.getRecordTypeName());
        Assert.thatUnchecked(viewPlan instanceof LogicalSortExpression);
        final var index = generator.generate(indexName, isUnique, table.getType(), containsNonNullableArray);
        final var tableWithIndex = RecordLayerTable.Builder
                .from(table)
                .addIndex(index)
                .build();
        context.asDdl().getMetadataBuilder().addTable(tableWithIndex);
        return null;
    }

    @Override
    public QueryPlan.LogicalQueryPlan visitInsertStatement(RelationalParser.InsertStatementContext ctx) {
        Assert.notNullUnchecked(ctx.tableName());
        final var targetTypeName = ((QualifiedIdentifierValue) ctx.tableName().accept(this)).getLiteralValue();
        final var maybeTargetType = context.asDql().getRecordLayerSchemaTemplate().findTableByName(targetTypeName).map(t -> ((RecordLayerTable) t).getType());
        Assert.thatUnchecked(maybeTargetType.isPresent(), String.format("Unknown table '%s'", targetTypeName), ErrorCode.UNDEFINED_TABLE);
        final var targetType = (Type.Record) maybeTargetType.get();

        // (yhatem): we should reorganize the INSERT parser rules such that we can easily change the DQL/DML context _here_.
        final var lookahead = ctx.insertStatementValue().start.getType();
        final var isInsertFromSelect = lookahead == RelationalLexer.SELECT;
        Assert.thatUnchecked(!isInsertFromSelect || ctx.columns == null, "setting column ordering for insert with select is not supported", ErrorCode.UNSUPPORTED_OPERATION);
        RelationalExpression fromExpression;

        if (isInsertFromSelect) {
            fromExpression = (RelationalExpression) ctx.insertStatementValue().accept(this);
        } else {
            final var targetTypeReorderings =
                    ctx.columns == null ?
                            null :
                            visitUidListWithNestingsInParens(ctx.columns);

            final var dmlContext = context.pushDmlContext();
            dmlContext.setTargetType(targetType);
            if (targetTypeReorderings != null) {
                dmlContext.setTargetTypeReorderings(targetTypeReorderings);
            }
            fromExpression = (RelationalExpression) visit(ctx.insertStatementValue());
            context.pop();
        }

        final var inQuantifier = Quantifier.forEach(GroupExpressionRef.of(fromExpression));
        RelationalExpression expression = new InsertExpression(inQuantifier, targetTypeName, targetType, context.asDql().getRecordLayerSchemaTemplate().getDescriptor(targetTypeName));
        // TODO Ask hatyo to help get rid of this hack.
        if (scopes.getCurrentScope() == null) {
            expression = new LogicalSortExpression(ImmutableList.of(), false, Quantifier.forEach(GroupExpressionRef.of(expression)));
        }
        return QueryPlan.LogicalQueryPlan.of(expression, context, query);
    }

    @Override
    public Object visitInsertStatementValueSelect(RelationalParser.InsertStatementValueSelectContext ctx) {
        // TODO There is some code that decides to create or not to create a logical sort expression based on
        //      whether the context is root or not. I don't think it should work like that.
        return ctx.selectStatement().accept(this);
    }

    @Override
    public Object visitInsertStatementValueValues(RelationalParser.InsertStatementValueValuesContext ctx) {
        final var valuesBuilder = ImmutableList.<Value>builder();
        Assert.thatUnchecked(!ctx.recordConstructorForInsert().isEmpty());
        for (final var recordConstructorUnambiguous : ctx.recordConstructorForInsert()) {
            final var recordValue = (Value) visit(recordConstructorUnambiguous);
            final var resultType = recordValue.getResultType();
            Assert.thatUnchecked(resultType instanceof Type.Record, "Records in a values statement have to be of record type.", ErrorCode.CANNOT_CONVERT_TYPE);
            valuesBuilder.add(recordValue);
        }

        final var arrayArgumentValues = valuesBuilder.build();
        final var arrayConstructorFn = FunctionCatalog.resolve("array", arrayArgumentValues.size())
                .orElseThrow(() -> Assert.failUnchecked("unable to resolve internal function"));
        final var insertValues = (Value) arrayConstructorFn.encapsulate(arrayArgumentValues);
        return new ExplodeExpression(insertValues);
    }

    @Override
    public Object visitUpdateStatement(RelationalParser.UpdateStatementContext ctx) {
        RelationalExpression expression = handleUpdateStatement(ctx.uid(),
                ctx.tableName(),
                ctx.updatedElement(),
                ctx.expression());

        if (ctx.RETURNING() != null) {
            final var qun = Quantifier.forEach(GroupExpressionRef.of(expression));
            final var scope = scopes.push();
            final var newFieldsSelector = FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(qun), 1);
            expression = new SelectExpression(newFieldsSelector, List.of(qun), List.of());
            final var selectQun = Quantifier.forEach(GroupExpressionRef.of(expression));
            scope.addQuantifier(selectQun);
            visit(ctx.selectElements());
            return QueryPlan.LogicalQueryPlan.of(scopes.pop().convertToRelationalExpression(), context, query);
        }

        // TODO Ask hatyo to help get rid of this hack.
        if (scopes.getCurrentScope() == null) {
            expression = new LogicalSortExpression(ImmutableList.of(), false, Quantifier.forEach(GroupExpressionRef.of(expression)));
        }

        return QueryPlan.LogicalQueryPlan.of(expression, context, query);
    }

    @Nonnull
    private UpdateExpression handleUpdateStatement(final RelationalParser.UidContext aliasCtx,
                                                   final RelationalParser.TableNameContext tableNameCtx,
                                                   final List<RelationalParser.UpdatedElementContext> updatedElementCtxs,
                                                   final RelationalParser.ExpressionContext expressionCtx) {
        final Scopes.Scope updateScope = scopes.push();

        final Typed tableNameTyped = (Typed) tableNameCtx.accept(this);
        Assert.thatUnchecked(tableNameTyped instanceof QualifiedIdentifierValue);
        final var targetTypeName = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(tableNameTyped, String.class));

        final var quantifierAlias =
                Assert.notNullUnchecked(
                        aliasCtx != null ?
                                ParserUtils.safeCastLiteral(visit(aliasCtx), String.class) :
                                targetTypeName);
        final var aliasId = CorrelationIdentifier.of(quantifierAlias);
        var fromExpression = ParserUtils.quantifyOver((QualifiedIdentifierValue) tableNameTyped, context, scopes, new AccessHints());
        var quantifier = Quantifier.forEachBuilder().withAlias(aliasId).build(GroupExpressionRef.of(fromExpression));
        updateScope.addQuantifier(quantifier);

        context.pushDmlContext();

        Assert.thatUnchecked(!updatedElementCtxs.isEmpty());
        final var transformMapBuilder = ImmutableMap.<FieldValue.FieldPath, Value>builder();
        for (final var updatedElementCtx : updatedElementCtxs) {
            final var updatedElementsPair = visitUpdatedElement(updatedElementCtx);
            transformMapBuilder.put(updatedElementsPair.getKey(), updatedElementsPair.getValue());
        }

        context.pop();

        if (expressionCtx != null) {
            final var predicateObj = expressionCtx.accept(this);
            Assert.notNullUnchecked(predicateObj);
            Assert.thatUnchecked(predicateObj instanceof Value, UNSUPPORTED_QUERY);
            final var booleanValue = (Value) predicateObj;
            Assert.thatUnchecked(booleanValue instanceof BooleanValue, String.format("unexpected predicate of type %s", booleanValue.getClass().getSimpleName()));
            final var innermostAliasOptional =
                    updateScope.getAllQuantifiers()
                            .stream()
                            .filter(qun -> qun instanceof Quantifier.ForEach)
                            .map(Quantifier::getAlias)
                            .findFirst();
            Assert.thatUnchecked(innermostAliasOptional.isPresent());
            final var predicate = LiteralsUtils.toQueryPredicate((BooleanValue) booleanValue, innermostAliasOptional.get(), context);
            updateScope.setPredicate(predicate);
        }

        final var expansionBuilder = GraphExpansion.builder();
        expansionBuilder.addAllQuantifiers(updateScope.getAllQuantifiers());
        if (updateScope.hasPredicate()) {
            expansionBuilder.addPredicate(updateScope.getPredicate());
        }

        final RelationalExpression filteredFromExpression =
                expansionBuilder.build().buildSelectWithResultValue(quantifier.getFlowedObjectValue());

        quantifier = Quantifier.forEach(GroupExpressionRef.of(filteredFromExpression));

        final var maybeTargetType = context.asDql().getRecordLayerSchemaTemplate().findTableByName(targetTypeName).map(t -> ((RecordLayerTable) t).getType());
        Assert.thatUnchecked(maybeTargetType.isPresent(), String.format("Unknown table '%s'", targetTypeName), ErrorCode.UNDEFINED_TABLE);
        final var targetType = (Type.Record) maybeTargetType.get();

        scopes.pop();

        return new UpdateExpression(quantifier,
                targetTypeName,
                targetType,
                context.asDql().getRecordLayerSchemaTemplate().getDescriptor(targetTypeName),
                transformMapBuilder.build());
    }

    @Override
    public Pair<FieldValue.FieldPath, Value> visitUpdatedElement(RelationalParser.UpdatedElementContext ctx) {
        final var fieldValue = (FieldValue) visit(ctx.fullColumnName());
        Assert.notNullUnchecked(fieldValue);
        final var newValue = (Value) visit(ctx.expression());
        Assert.notNullUnchecked(newValue);
        return Pair.of(fieldValue.getFieldPath(), newValue);
    }

    @Override
    public Object visitDeleteStatement(RelationalParser.DeleteStatementContext ctx) {
        RelationalExpression expression = handleDeleteStatement(ctx.tableName(), ctx.expression());

        if (ctx.RETURNING() != null) {
            final var scope = scopes.push();
            final var selectQun = Quantifier.forEach(GroupExpressionRef.of(expression));
            scope.addQuantifier(selectQun);
            visit(ctx.selectElements());
            return QueryPlan.LogicalQueryPlan.of(scopes.pop().convertToRelationalExpression(), context, query);
        }

        //// TODO Ask hatyo to help get rid of this hack.
        if (scopes.getCurrentScope() == null) {
            expression = new LogicalSortExpression(ImmutableList.of(), false, Quantifier.forEach(GroupExpressionRef.of(expression)));
        }
        return QueryPlan.LogicalQueryPlan.of(expression, context, query);
    }

    @Nonnull
    private DeleteExpression handleDeleteStatement(final RelationalParser.TableNameContext tableNameContext,
                                                   final RelationalParser.ExpressionContext expressionCtx) {
        final Scopes.Scope updateScope = scopes.push();

        final Typed tableNameTyped = (Typed) tableNameContext.accept(this);
        Assert.thatUnchecked(tableNameTyped instanceof QualifiedIdentifierValue);

        final var targetTypeName = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(tableNameTyped, String.class));

        final var aliasId = CorrelationIdentifier.of(targetTypeName);
        var fromExpression = ParserUtils.quantifyOver((QualifiedIdentifierValue) tableNameTyped, context, scopes, new AccessHints());
        var quantifier = Quantifier.forEachBuilder().withAlias(aliasId).build(GroupExpressionRef.of(fromExpression));
        updateScope.addQuantifier(quantifier);

        if (expressionCtx != null) {
            final var predicateObj = expressionCtx.accept(this);
            Assert.notNullUnchecked(predicateObj);
            Assert.thatUnchecked(predicateObj instanceof Value, UNSUPPORTED_QUERY);
            final var booleanValue = (Value) predicateObj;
            Assert.thatUnchecked(booleanValue instanceof BooleanValue, String.format("unexpected predicate of type %s", booleanValue.getClass().getSimpleName()));
            final var innermostAliasOptional =
                    updateScope.getAllQuantifiers()
                            .stream()
                            .filter(qun -> qun instanceof Quantifier.ForEach)
                            .map(Quantifier::getAlias)
                            .findFirst();
            Assert.thatUnchecked(innermostAliasOptional.isPresent());
            final var predicate = LiteralsUtils.toQueryPredicate((BooleanValue) booleanValue, innermostAliasOptional.get(), context);
            updateScope.setPredicate(predicate);
        }

        final var expansionBuilder = GraphExpansion.builder();
        expansionBuilder.addAllQuantifiers(updateScope.getAllQuantifiers());
        if (updateScope.hasPredicate()) {
            expansionBuilder.addPredicate(updateScope.getPredicate());
        }

        final RelationalExpression filteredFromExpression =
                expansionBuilder.build().buildSelectWithResultValue(quantifier.getFlowedObjectValue());

        quantifier = Quantifier.forEach(GroupExpressionRef.of(filteredFromExpression));

        scopes.pop();

        return new DeleteExpression(quantifier,
                targetTypeName);
    }

    @Override
    public ProceduralPlan visitDropDatabaseStatement(RelationalParser.DropDatabaseStatementContext ctx) {
        final String dbName = ParserUtils.safeCastLiteral(visit(ctx.path()), String.class);
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return  ProceduralPlan.of(context.asDdl().getMetadataOperationsFactory().getDropDatabaseConstantAction(URI.create(dbName), throwIfDoesNotExist, Options.NONE));
    }

    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(RelationalParser.DropSchemaTemplateStatementContext ctx) {
        final String id = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return ProceduralPlan.of(context.asDdl().getMetadataOperationsFactory().getDropSchemaTemplateConstantAction(Assert.notNullUnchecked(id), throwIfDoesNotExist, Options.NONE));
    }

    @Override
    public ProceduralPlan visitDropSchemaStatement(RelationalParser.DropSchemaStatementContext ctx) {
        final String schemaId = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class));
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(schemaId);
        Assert.thatUnchecked(dbAndSchema.getLeft().isPresent(), String.format("invalid database identifier in '%s'", ctx.uid().getText()));
        return ProceduralPlan.of(context.asDdl().getMetadataOperationsFactory().getDropSchemaConstantAction(dbAndSchema.getLeft().get(), dbAndSchema.getRight(), Options.NONE));
    }

    /////// administration statements //////////////

    @Override
    public Object visitShowDatabasesStatement(RelationalParser.ShowDatabasesStatementContext ctx) {
        context.pushDdlContext();
        if (ctx.path() != null) {
            final String dbName = ParserUtils.safeCastLiteral(visit(ctx.path()), String.class);
            Assert.notNullUnchecked(dbName);
            Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
            return QueryPlan.MetadataQueryPlan.of(ddlQueryFactory.getListDatabasesQueryAction(URI.create(dbName)));
        }
        return QueryPlan.MetadataQueryPlan.of(ddlQueryFactory.getListDatabasesQueryAction(dbUri));
    }

    @Override
    public QueryPlan visitShowSchemaTemplatesStatement(RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        return QueryPlan.MetadataQueryPlan.of(ddlQueryFactory.getListSchemaTemplatesQueryAction());
    }

    /////// utility statements /////////////////////

    @Override
    public QueryPlan visitSimpleDescribeSchemaStatement(RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        final String schemaId = ParserUtils.safeCastLiteral(visit(ctx.schemaId()), String.class);
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(Assert.notNullUnchecked(schemaId));
        return QueryPlan.MetadataQueryPlan.of(ddlQueryFactory.getDescribeSchemaQueryAction(dbAndSchema.getLeft().orElse(dbUri), dbAndSchema.getRight()));
    }

    @Override
    public QueryPlan visitSimpleDescribeSchemaTemplateStatement(RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        final var schemaTemplateName = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
        return QueryPlan.MetadataQueryPlan.of(ddlQueryFactory.getDescribeSchemaTemplateQueryAction(Assert.notNullUnchecked(schemaTemplateName)));
    }

    /**
     * Parses a query generating an equivalent abstract syntax tree.
     *
     * @param query The query.
     * @return The abstract syntax tree.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    public static RelationalParser.RootContext parseQuery(@Nonnull final String query) throws RelationalException {
        final RelationalLexer tokenSource = new RelationalLexer(new CaseInsensitiveCharStream(query));
        final RelationalParser parser = new RelationalParser(new CommonTokenStream(tokenSource));
        parser.removeErrorListeners();
        final SyntaxErrorListener listener = new SyntaxErrorListener();
        parser.addErrorListener(listener);
        RelationalParser.RootContext rootContext = parser.root();
        if (!listener.getSyntaxErrors().isEmpty()) {
            throw listener.getSyntaxErrors().get(0).toRelationalException();
        }
        return rootContext;
    }
}
