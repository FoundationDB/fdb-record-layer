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

import com.apple.foundationdb.record.RecordMetaDataProto;
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ExistsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.ConstantActionFactory;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.net.URI;
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
 */
public class AstVisitor extends RelationalParserBaseVisitor<Object> {

    @Nonnull
    private RelationalParserContext parserContext;

    @Nonnull
    private final String query;

    @Nonnull
    private final ConstantActionFactory constantActionFactory;

    @Nonnull
    private final URI dbUri;

    @Nonnull
    private final TypingContext typingContext;

    public static final String UNSUPPORTED_QUERY = "query is not supported";

    @Nonnull
    private final DdlQueryFactory ddlQueryFactory;

    /*
    whether the schema contains Non-nullable array.
    If it is false, we'll wrap array field in all indexes. If it is true, we won't wrap any array field in any indexes.
    It is a temporary work-around. We should wrap nullable array field in indexes. It'll be removed after this work is done.
    */
    private boolean containsNonNullableArray;

    /**
     * Creates a new instance of {@link AstVisitor}.
     *
     * @param parserContext         The parsing context used to maintain references.
     * @param query                 The string representation of the SQL statement.
     * @param constantActionFactory A factory used to run DDL statements with side-effects.
     * @param ddlQueryFactory       A factory used to query the metadata.
     * @param dbUri                 The current database {@link URI}.
     */
    public AstVisitor(@Nonnull final RelationalParserContext parserContext,
                      @Nonnull final String query,
                      @Nonnull final ConstantActionFactory constantActionFactory,
                      @Nonnull final DdlQueryFactory ddlQueryFactory,
                      @Nonnull final URI dbUri) {
        this.query = query;
        this.parserContext = parserContext;
        this.constantActionFactory = constantActionFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
        this.typingContext = TypingContext.create();
        this.containsNonNullableArray = false;
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        return nextResult != null ? nextResult : aggregate;
    }

    @Override
    public QueryPlan visitDmlStatement(RelationalParser.DmlStatementContext ctx) {
        Assert.thatUnchecked(ctx.selectStatementWithContinuation() != null || ctx.explainStatement() != null, UNSUPPORTED_QUERY);
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
            final String continuationStr = ParserUtils.safeCastLiteral(ctx.stringLiteral().accept(this), String.class);
            Assert.notNullUnchecked(continuationStr, "continuation can not be null.");
            Assert.thatUnchecked(parserContext.getOffset() == 0, "Offset cannot be specified with continuation.");
            return QueryPlan.QpQueryplan.of(result, query, parserContext.getLimit(), parserContext.getOffset(),
                    Base64.getDecoder().decode(continuationStr));
        } else {
            return QueryPlan.QpQueryplan.of(result, query, parserContext.getLimit(), parserContext.getOffset());
        }
    }

    @Override
    public QueryPlan visitExplainStatement(RelationalParser.ExplainStatementContext ctx) {
        Assert.notNullUnchecked(ctx.selectStatement(), UNSUPPORTED_QUERY);
        RelationalExpression result = (RelationalExpression) ctx.selectStatement().accept(this);
        Assert.thatUnchecked(query.stripLeading().toUpperCase(Locale.ROOT).startsWith("EXPLAIN"));
        return new QueryPlan.ExplainPlan(QueryPlan.QpQueryplan.of(result, query.stripLeading().substring(7),
                parserContext.getLimit(), parserContext.getOffset()));
    }

    @Override
    public RelationalExpression visitParenthesisSelect(RelationalParser.ParenthesisSelectContext ctx) {
        Assert.isNullUnchecked(ctx.lockClause(), UNSUPPORTED_QUERY);
        return (RelationalExpression) ctx.queryExpression().accept(this);
    }

    @Override
    public RelationalExpression visitSimpleSelect(RelationalParser.SimpleSelectContext ctx) {
        Assert.isNullUnchecked(ctx.lockClause(), UNSUPPORTED_QUERY);
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

    @ExcludeFromJacocoGeneratedReport // not reachable for now, but planned.
    @Override
    public RelationalExpression visitQueryExpressionNointo(RelationalParser.QueryExpressionNointoContext ctx) {
        if (ctx.queryExpressionNointo() != null) {
            return (RelationalExpression) (visit(ctx.queryExpressionNointo())); // recursive
        }
        return (RelationalExpression) visit(ctx.querySpecificationNointo());
    }

    @Override
    public RelationalExpression visitQuerySpecification(RelationalParser.QuerySpecificationContext ctx) {
        Assert.thatUnchecked(ctx.selectSpec().isEmpty(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.havingClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.windowClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.orderByClause(), UNSUPPORTED_QUERY);
        Assert.notNullUnchecked(ctx.fromClause(), UNSUPPORTED_QUERY);

        return handleSelectInternal(ctx.selectElements(), ctx.fromClause(), ctx.groupByClause(), ctx.orderByClause(), ctx.limitClause());
    }

    @Override
    public RelationalExpression visitQuerySpecificationNointo(RelationalParser.QuerySpecificationNointoContext ctx) {
        Assert.thatUnchecked(ctx.selectSpec().isEmpty(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.havingClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.windowClause(), UNSUPPORTED_QUERY);
        //Assert.isNullUnchecked(ctx.orderByClause(), UNSUPPORTED_QUERY);
        Assert.notNullUnchecked(ctx.fromClause(), UNSUPPORTED_QUERY);

        return handleSelectInternal(ctx.selectElements(), ctx.fromClause(), ctx.groupByClause(), ctx.orderByClause(), ctx.limitClause());
    }

    private RelationalExpression handleSelectInternal(@Nonnull final RelationalParser.SelectElementsContext selectElements,
                                                      @Nonnull final RelationalParser.FromClauseContext fromClause,
                                                      @Nullable final RelationalParser.GroupByClauseContext groupByClause,
                                                      @Nullable final RelationalParser.OrderByClauseContext orderByClause,
                                                      @Nullable final RelationalParser.LimitClauseContext limitClause) {
        if (groupByClause != null || ParserUtils.hasAggregation(selectElements)) {
            return handleSelectWithGroupBy(selectElements, fromClause, groupByClause, orderByClause, limitClause);
        } else {
            parserContext.siblingScope();
            fromClause.accept(this); // includes checking predicates
            selectElements.accept(this); // potentially sets explicit result columns on top-level select expression.
            if (orderByClause != null) {
                visit(orderByClause);
            }
            if (limitClause != null) {
                visit(limitClause);
            }
            return parserContext.popScope().convertToRelationalExpression();
        }
    }

    @Nonnull
    private RelationalExpression handleSelectWithGroupBy(@Nonnull final RelationalParser.SelectElementsContext selectElements,
                                                         @Nonnull final RelationalParser.FromClauseContext fromClause,
                                                         @Nullable final RelationalParser.GroupByClauseContext groupByClause,
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

        Quantifier underlyingSelectQun = null;
        // 1. create a new SELECT expression projecting all underlying quantifiers.
        {
            final var builder = GraphExpansion.builder();
            // handle underlying select.
            parserContext.pushScope();
            // grab all quantifiers into the current scope.
            fromClause.accept(this);
            final var scope = parserContext.getCurrentScope();
            final var columns = scope.getAllQuantifiers().stream().map(qun -> {
                final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
                return Column.of(
                        Type.Record.Field.of(
                                quantifiedValue.getResultType(),
                                Optional.of(qun.getAlias().getId())),
                        quantifiedValue);
            });

            RecordConstructorValue groupingColumnsValue = null;
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
            final var resultColumns = Streams.concat(
                    Stream.of(Column.unnamedOf(groupingColumnsValue)),
                    columns).collect(Collectors.toList());
            builder.addAllQuantifiers(scope.getAllQuantifiers()).addAllResultColumns(resultColumns);
            if (scope.hasPredicate()) {
                builder.addPredicate(scope.getPredicate());
            }
            parserContext.popScope();
            underlyingSelectQun = Quantifier.forEach(GroupExpressionRef.of(builder.build().buildSelect()));
            parserContext.pushScope().addQuantifier(underlyingSelectQun); // for later processing
        }

        // 2. handle group by expression.
        Quantifier groupByQuantifier = null;
        Type groupByExpressionType = null;
        Optional<List<Column<? extends Value>>> orderByColumns = Optional.empty();
        {
            final var scope = parserContext.getCurrentScope();
            scope.setFlag(Scopes.Scope.Flag.UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE); // set this flag, so we can resolve grouping identifiers correctly.
            selectElements.accept(this); // add aggregations (make a list of individual aggregations for the Agg expression) and other (group by) columns.
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
            final var aggregationValue = RecordConstructorValue.ofColumns(aggregateValues.stream().map(agg ->
                    Column.of(Type.Record.Field.of(agg.getResultType(), Optional.of(ParserUtils.toProtoBufCompliantName(CorrelationIdentifier.uniqueID().getId()))), agg)).collect(Collectors.toList()));
            final var groupByExpression = new GroupByExpression(aggregationValue, groupByClause == null ? null : FieldValue.ofOrdinalNumber(underlyingSelectQun.getFlowedObjectValue(), 0), underlyingSelectQun);
            groupByExpressionType = groupByExpression.getResultValue().getResultType();
            parserContext.popScope();
            groupByQuantifier = Quantifier.forEach(GroupExpressionRef.of(groupByExpression), groupByQunAlias);
            parserContext.pushScope().addQuantifier(groupByQuantifier);
        }

        // 3. handle select on top of group by
        {
            final var scope = parserContext.getCurrentScope();
            scope.setFlag(Scopes.Scope.Flag.UNDERLYING_EXPRESSION_HAS_GROUPING_VALUE); // set this flag, so we can resolve grouping identifiers correctly.
            scope.setFlag(Scopes.Scope.Flag.RESOLVING_SELECT_HAVING); // set this flag, so we transform aggregate values into references.
            scope.setGroupByQuantifierCorrelation(groupByQunAlias);
            scope.setGroupByType(groupByExpressionType);
            scope.getProjectList().clear();
            visit(selectElements);
            orderByColumns.ifPresent(columns -> columns.forEach(scope::addOrderByColumn));
            if (limitClause != null) {
                visit(limitClause);
            }
            return parserContext.popScope().convertToRelationalExpression();
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
        return fieldNames.stream().map(fieldName -> ParserUtils.resolveField(List.of(fieldName), parserContext)).map(ParserUtils::toColumn).collect(Collectors.toList());
    }

    @Nonnull
    private List<Column<? extends Value>> expandStarColumns(Value id) {
        final var scope = parserContext.getCurrentScope();
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
            final var groupByQun = parserContext.getCurrentScope().getForEachQuantifiers().get(0);
            final var selectWhereQun = groupByQun.getRangesOver().get().getQuantifiers().stream().filter(q -> q instanceof Quantifier.ForEach).collect(Collectors.toList());
            Assert.thatUnchecked(selectWhereQun.size() == 1);
            return expandSelectWhereColumns(selectWhereQun.get(0), id);

        } else if (isUnderlyingSelectWhere) { // expand { GB, -> {... rest} }
            Assert.thatUnchecked(scope.getForEachQuantifiers().size() == 1);
            final var qun = parserContext.getCurrentScope().getForEachQuantifiers().get(0);
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
            final var scope = parserContext.getCurrentScope();
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
        final var scope = parserContext.getCurrentScope();
        cols.forEach(scope::addProjectionColumn);
        return null;
    }

    @Override
    public Void visitSelectColumnElement(RelationalParser.SelectColumnElementContext ctx) {
        Column<Value> column;
        if (ctx.uid() != null) {
            column = ParserUtils.toColumn((Value) ctx.fullColumnName().accept(this),
                    Objects.requireNonNull(ParserUtils.safeCastLiteral(ctx.uid().accept(this), String.class)));
        } else {
            column = ParserUtils.toColumn((Value) ctx.fullColumnName().accept(this));
        }
        parserContext.getCurrentScope().addProjectionColumn(column);
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
        parserContext.getCurrentScope().addProjectionColumn(column);
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport // can not reach code path for now
    public Void visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext ctx) {
        Assert.isNullUnchecked(ctx.LOCAL_ID(), UNSUPPORTED_QUERY);
        final var expressionObj = ctx.expression().accept(this);
        Assert.thatUnchecked(expressionObj instanceof Value, UNSUPPORTED_QUERY);
        final var expression = (Value) expressionObj;
        Column<Value> column;
        if (ctx.AS() != null) {
            final String alias = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
            Assert.notNullUnchecked(alias, UNSUPPORTED_QUERY);
            column = Column.of(Type.Record.Field.of(expression.getResultType(), Optional.of(alias), Optional.empty()), expression);
        } else {
            column = Column.unnamedOf(expression);
        }
        parserContext.getCurrentScope().addProjectionColumn(column);
        return null;
    }

    @Override
    public Void visitOrderByClause(RelationalParser.OrderByClauseContext ctx) {
        ctx.orderByExpression().forEach(expression -> {
            Column<? extends Value> column = (Column<? extends Value>) expression.accept(this);
            parserContext.getCurrentScope().addOrderByColumn(column);
        });
        return null;
    }

    @Override
    public Column<Value> visitOrderByExpression(RelationalParser.OrderByExpressionContext ctx) {
        Assert.isNullUnchecked(ctx.DESC(), "Unsupported DESC order", ErrorCode.UNSUPPORTED_OPERATION);
        return ParserUtils.toColumn((Value) ctx.expression().accept(this));
    }

    @Override
    public Void visitFromClause(RelationalParser.FromClauseContext ctx) {
        Assert.notNullUnchecked(ctx.FROM(), UNSUPPORTED_QUERY);
        // prepare parser context for resolving aliases by parsing FROM clause first.
        ctx.tableSources().accept(this);
        if (ctx.WHERE() != null) {
            final var predicateObj = ctx.whereExpr.accept(this);
            Assert.notNullUnchecked(predicateObj);
            Assert.thatUnchecked(predicateObj instanceof Value, UNSUPPORTED_QUERY);
            final Value predicate = (Value) predicateObj;
            Assert.thatUnchecked(predicate instanceof BooleanValue, String.format("unexpected predicate of type %s", predicate.getClass().getSimpleName()));
            final Collection<CorrelationIdentifier> aliases = parserContext.getCurrentScope().getAllQuantifiers().stream().filter(qun -> qun instanceof Quantifier.ForEach).map(Quantifier::getAlias).collect(Collectors.toList()); // not sure this is correct
            Assert.thatUnchecked(!aliases.isEmpty());
            final Optional<QueryPredicate> predicateOptional = ((BooleanValue) predicate).toQueryPredicate(aliases.stream().findFirst().get());
            Assert.thatUnchecked(predicateOptional.isPresent(), "query is not supported");
            parserContext.getCurrentScope().setPredicate(predicateOptional.get()); // improve
        }
        return null;
    }

    @Override
    public Void visitGroupByClause(RelationalParser.GroupByClauseContext ctx) {
        final var scope = parserContext.getCurrentScope();
        for (final var groupingCol : ctx.groupByItem()) {
            scope.addProjectionColumn((Column<? extends Value>) groupingCol.accept(this));
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
        var parentScope = parserContext.getCurrentScope().getParent();
        var siblingScope = parserContext.getCurrentScope().getSibling();
        Assert.thatUnchecked(parentScope == null && siblingScope == null,
                "LIMIT clause can only be specified with top-level SQL query.", ErrorCode.UNSUPPORTED_OPERATION);
        parserContext.setLimit(ParserUtils.parseBoundInteger(ctx.limit.getText(), 1, null));
        if (ctx.offset != null) {
            parserContext.setOffset(ParserUtils.parseBoundInteger(ctx.offset.getText(), 0, null));
        }
        // Owing to TODO
        if (parserContext.getCurrentScope().getAllQuantifiers().size() > 1) {
            Assert.thatUnchecked(parserContext.getLimit() == 0 && parserContext.getOffset() == 0,
                    "LIMIT / OFFSET with multiple FROM elements is not supported.", ErrorCode.UNSUPPORTED_OPERATION);
        }
        return null;
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
        final Set<String> allIndexes = parserContext.getIndexNames();
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

        final RelationalExpression from = ParserUtils.quantifyOver((QualifiedIdentifierValue) tableName, parserContext, new AccessHints(accessHintSet.toArray(AccessHint[]::new)));
        final var quantifierAlias = ctx.alias != null ?
                ParserUtils.safeCastLiteral(visit(ctx.alias), String.class) :
                ParserUtils.safeCastLiteral(tableName, String.class);
        final CorrelationIdentifier aliasId = CorrelationIdentifier.of(quantifierAlias);
        final Quantifier.ForEach forEachQuantifier = Quantifier.forEachBuilder().withAlias(aliasId).build(GroupExpressionRef.of(from));
        parserContext.getCurrentScope().addQuantifier(forEachQuantifier);
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
        parserContext.getCurrentScope().addQuantifier(forEachQuantifier);
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
        Assert.thatUnchecked(ctx.logicalOperator().OR() != null || ctx.logicalOperator().AND() != null, String.format("logical operator %s is not supported", ctx.logicalOperator().getText()));
        final Value left = (Value) (visit(ctx.expression(0)));
        final Value right = (Value) (visit(ctx.expression(1)));

        if (ctx.logicalOperator().AND() != null) {
            return (Value) (new AndOrValue.AndFn().encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left, right)));
        }
        return (Value) (new AndOrValue.OrFn().encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left, right)));
    }

    @ExcludeFromJacocoGeneratedReport
    // remove once code branch is testable after fixing nullability bug in record layer
    @Override
    public Value visitIsExpression(RelationalParser.IsExpressionContext ctx) {
        Assert.isNullUnchecked(ctx.UNKNOWN(), UNSUPPORTED_QUERY);
        final Value left = (Value) (visit(ctx.predicate()));
        if (ctx.FALSE() == null && ctx.TRUE() == null) {
            Assert.failUnchecked(String.format("unexpected value %s", ctx.getText()));
        }
        final Value right = (ctx.TRUE() != null) ?
                new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true) :
                new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
        if (ctx.NOT() != null) {
            return (Value) (new RelOpValue.NotEqualsFn().encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left, right)));
        } else {
            return (Value) (new RelOpValue.EqualsFn().encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left, right)));
        }
    }

    ///// Predicates ///////

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitInPredicate(RelationalParser.InPredicateContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Typed visitIsNullPredicate(RelationalParser.IsNullPredicateContext ctx) {
        final Value left = (Value) (visit(ctx.predicate()));
        if (ctx.NOT() != null) {
            return new RelOpValue.NotNullFn().encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left));
        } else {
            return new RelOpValue.IsNullFn().encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left));
        }
    }

    @Override
    public Value visitBinaryComparisonPredicate(RelationalParser.BinaryComparisonPredicateContext ctx) {
        final Value left = (Value) (visit(ctx.left));
        final Value right = (Value) (visit(ctx.right));
        BuiltInFunction<? extends Value> comparisonFunction = ParserUtils.getFunction(ctx.comparisonOperator().getText());
        return (Value) (comparisonFunction.encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left, right)));
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitSubqueryComparisonPredicate(RelationalParser.SubqueryComparisonPredicateContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitBetweenPredicate(RelationalParser.BetweenPredicateContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitSoundsLikePredicate(RelationalParser.SoundsLikePredicateContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitLikePredicate(RelationalParser.LikePredicateContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitRegexpPredicate(RelationalParser.RegexpPredicateContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitExpressionAtomPredicate(RelationalParser.ExpressionAtomPredicateContext ctx) {
        Assert.isNullUnchecked(ctx.LOCAL_ID(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.VAR_ASSIGN(), UNSUPPORTED_QUERY);
        return (Value) visit(ctx.expressionAtom());
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Typed visitJsonMemberOfPredicate(RelationalParser.JsonMemberOfPredicateContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    ///// Expression Atoms //////

    @Override
    public Value visitFunctionCallExpressionAtom(RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return (Value) visitChildren(ctx);
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitCollateExpressionAtom(RelationalParser.CollateExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitUnaryExpressionAtom(RelationalParser.UnaryExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitBinaryExpressionAtom(RelationalParser.BinaryExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitNestedExpressionAtom(RelationalParser.NestedExpressionAtomContext ctx) {
        if (ctx.expression().size() == 1) {
            return (Value) (ctx.expression(0).accept(this));
        } else {
            return RecordConstructorValue.ofUnnamed(ctx.expression().stream().map(e -> (Value) (visit(e))).collect(Collectors.toUnmodifiableList()));
        }
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitNestedRowExpressionAtom(RelationalParser.NestedRowExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitExistsExpressionAtom(RelationalParser.ExistsExpressionAtomContext ctx) {
        Assert.notNullUnchecked(ctx.selectStatement());
        parserContext.pushScope();
        final var expression = ctx.selectStatement().accept(this);
        parserContext.popScope();
        Assert.thatUnchecked(expression instanceof RelationalExpression);
        final RelationalExpression nestedSubquery = (RelationalExpression) expression;
        Assert.notNullUnchecked(nestedSubquery);
        final Quantifier.Existential existsQuantifier = Quantifier.existential(GroupExpressionRef.of(nestedSubquery));
        parserContext.getCurrentScope().addQuantifier(existsQuantifier);
        return new ExistsValue(existsQuantifier.getFlowedObjectValue());
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitSubqueryExpressionAtom(RelationalParser.SubqueryExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitBitExpressionAtom(RelationalParser.BitExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitMathExpressionAtom(RelationalParser.MathExpressionAtomContext ctx) {
        final Value left = (Value) (visit(ctx.left));
        final Value right = (Value) (visit(ctx.right));
        BuiltInFunction<? extends Value> mathFunction = ParserUtils.getFunction(ctx.mathOperator().getText());
        return (Value) (mathFunction.encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(left, right)));
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitJsonExpressionAtom(RelationalParser.JsonExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    ////// DB Objects /////


    @Override
    public Value visitFullId(RelationalParser.FullIdContext ctx) {
        Assert.thatUnchecked(!ctx.uid().isEmpty());
        Assert.thatUnchecked(ctx.uid().size() > 0);
        final List<String> ids = ctx.uid().stream().map(this::visit).map(f -> ParserUtils.safeCastLiteral(f, String.class)).map(Assert::notNullUnchecked).collect(Collectors.toList());
        return QualifiedIdentifierValue.of(ids.toArray(new String[0]));
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
        return ParserUtils.resolveField(fieldParts, parserContext);
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
    public Value visitUserName(RelationalParser.UserNameContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitMysqlVariable(RelationalParser.MysqlVariableContext ctx) {
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

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitUuidSet(RelationalParser.UuidSetContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitXid(RelationalParser.XidContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitXuidStringId(RelationalParser.XuidStringIdContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitAuthPlugin(RelationalParser.AuthPluginContext ctx) {
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

    //// Literals ////


    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitFileSizeLiteral(RelationalParser.FileSizeLiteralContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Value visitNegativeDecimalConstant(RelationalParser.NegativeDecimalConstantContext ctx) {
        return ParserUtils.parseDecimal(ctx.decimalLiteral().getText(), true);
    }

    @Override
    public Value visitNullConstant(RelationalParser.NullConstantContext ctx) {
        Assert.isNullUnchecked(ctx.NOT(), UNSUPPORTED_QUERY);
        return (Value) (visit(ctx.nullLiteral()));
    }

    @Override
    public Value visitNullLiteral(RelationalParser.NullLiteralContext ctx) {
        return new LiteralValue<>(null); // warning: UNKNOWN type
    }

    @Override
    public Value visitBooleanLiteral(RelationalParser.BooleanLiteralContext ctx) {
        if (ctx.FALSE() != null) {
            return new LiteralValue<>(false);
        } else {
            Assert.notNullUnchecked(ctx.TRUE(), String.format("unexpected boolean value %s", ctx.getText()));
            return new LiteralValue<>(true);
        }
    }

    @Override
    public Value visitHexadecimalLiteral(RelationalParser.HexadecimalLiteralContext ctx) {
        Assert.isNullUnchecked(ctx.STRING_CHARSET_NAME(), UNSUPPORTED_QUERY);
        Assert.notNullUnchecked(ctx.HEXADECIMAL_LITERAL(), UNSUPPORTED_QUERY);
        return new LiteralValue<>(new BigInteger(ctx.HEXADECIMAL_LITERAL().getText().substring(2), 16).longValue());
    }

    @Override
    public Value visitStringLiteral(RelationalParser.StringLiteralContext ctx) {
        Assert.isNullUnchecked(ctx.STRING_CHARSET_NAME(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.START_NATIONAL_STRING_LITERAL(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.COLLATE(), UNSUPPORTED_QUERY);
        return new LiteralValue<>(ParserUtils.normalizeString(ctx.getText()));
    }

    @Override
    public Value visitDecimalLiteral(RelationalParser.DecimalLiteralContext ctx) {
        return ParserUtils.parseDecimal(ctx.getText(), false);
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
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Object visitUdfFunctionCall(RelationalParser.UdfFunctionCallContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Object visitPasswordFunctionCall(RelationalParser.PasswordFunctionCallContext ctx) {
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

        final var scope = parserContext.getCurrentScope();
        if (scope.isFlagSet(Scopes.Scope.Flag.RESOLVING_SELECT_HAVING)) {
            final var groupByQuantifier = QuantifiedObjectValue.of(Objects.requireNonNull(scope.getGroupByQuantifierCorrelation()), Objects.requireNonNull(scope.getGroupByType()));
            final var aggExprSelector = FieldValue.ofOrdinalNumber(groupByQuantifier, ParserUtils.getLastFieldIndex(groupByQuantifier.getResultType()));
            final var aggregationReference = FieldValue.ofOrdinalNumber(aggExprSelector, scope.getAggCounter());
            scope.increaseAggCounter();
            return aggregationReference;
        } else {
            final var functionName = ctx.functionName.getText();
            final var aggregationFunction = ParserUtils.getFunction(functionName);
            if (ctx.starArg != null) {
                final var aggregationValue = (AggregateValue) (aggregationFunction.encapsulate(parserContext.getTypeRepositoryBuilder(), List.of(RecordConstructorValue.ofColumns(List.of()))));
                scope.addAggregateValue(aggregationValue);
                return aggregationValue;
            } else if (ctx.functionArg() != null) {
                scope.setFlag(Scopes.Scope.Flag.RESOLVING_AGGREGATION);
                final var argument = ctx.functionArg().accept(this);
                scope.unsetFlag(Scopes.Scope.Flag.RESOLVING_AGGREGATION);
                final var aggregationValue = (AggregateValue) (aggregationFunction.encapsulate(parserContext.getTypeRepositoryBuilder(), List.of((Value) argument)));
                scope.addAggregateValue(aggregationValue);
                return aggregationValue;
            } else {
                final var aggregationValue = (AggregateValue) (aggregationFunction.encapsulate(parserContext.getTypeRepositoryBuilder(), List.of()));
                scope.addAggregateValue(aggregationValue);
                return aggregationValue;
            }
        }
    }

    /////// DDL statements //////////////

    @Override
    public ProceduralPlan visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        Assert.thatUnchecked(ctx.createStatement() != null || ctx.dropStatement() != null, UNSUPPORTED_QUERY);
        return (ProceduralPlan) visitChildren(ctx);
    }

    @Override
    public ProceduralPlan visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx) {
        final String schemaId = ParserUtils.safeCastLiteral(visit(ctx.schemaId()), String.class);
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(schemaId);
        final String templateId = ParserUtils.safeCastLiteral(visit(ctx.templateId()), String.class);
        return ProceduralPlan.of(constantActionFactory.getCreateSchemaConstantAction(dbAndSchema.getLeft().orElse(dbUri),
                dbAndSchema.getRight(), templateId, Options.NONE));
    }

    @Override
    public Void visitTemplateClause(RelationalParser.TemplateClauseContext ctx) {
        if (ctx.structOrTableDefinition() != null) {
            ctx.structOrTableDefinition().accept(this);
        } else if (ctx.enumDefinition() != null) {
            ctx.enumDefinition().accept(this);
        } else {
            typingContext.addAllToTypeRepository();
            // reset metadata such that we can use it to resolvae identifiers in subsequent materialized view definition(s).
            parserContext = parserContext.withTypeRepositoryBuilder(typingContext.getTypeRepositoryBuilder())
                    .withScannableRecordTypes(typingContext.getTableNames(), typingContext.getFieldDescriptorMap())
                    .withIndexNames(typingContext.getIndexNames());
            visit(ctx.indexDefinition());
        }
        return null;
    }

    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        final var schemaTemplateName = ParserUtils.safeCastLiteral(visit(ctx.schemaTemplateId()), String.class);

        // collect all tables, their indices, and custom types definitions.
        ctx.templateClause().forEach(s -> s.accept(this));
        typingContext.addAllToTypeRepository();
        // reset metadata such that we can use it to resolve identifiers in subsequent index definition(s).
        parserContext = parserContext.withTypeRepositoryBuilder(typingContext.getTypeRepositoryBuilder())
                .withScannableRecordTypes(typingContext.getTableNames(), typingContext.getFieldDescriptorMap())
                .withIndexNames(typingContext.getIndexNames());

        // schema template version will be set automatically at update operation to lastVersion + 1
        SchemaTemplate schemaTemplate = typingContext.generateSchemaTemplate(schemaTemplateName, 1L);
        return ProceduralPlan.of(constantActionFactory.getCreateSchemaTemplateConstantAction(schemaTemplate, Options.NONE));
    }

    @Override
    public ProceduralPlan visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
        final String dbName = ParserUtils.safeCastLiteral(visit(ctx.path()), String.class);
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        return ProceduralPlan.of(constantActionFactory.getCreateDatabaseConstantAction(URI.create(dbName), Options.NONE));
    }

    @Override
    public Void visitStructOrTableDefinition(RelationalParser.StructOrTableDefinitionContext ctx) {
        Assert.thatUnchecked(ctx.STRUCT() == null || ctx.primaryKeyDefinition() == null,
                String.format("Illegal struct definition '%s'", ctx.uid().getText()), ErrorCode.SYNTAX_ERROR);
        Assert.thatUnchecked(ctx.TABLE() == null || ctx.primaryKeyDefinition() != null,
                String.format("Illegal table definition '%s'. Include either a PRIMARY KEY clause OR A SINGLE ROW ONLY clause.", ctx.uid().getText()), ErrorCode.SYNTAX_ERROR);
        final var name = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
        final List<TypingContext.FieldDefinition> fields = ctx.columnDefinition().stream().map(c ->
                (TypingContext.FieldDefinition) c.accept(this)).collect(Collectors.toList());
        final var isTable = ctx.STRUCT() == null;
        if (ctx.primaryKeyDefinition() != null) {
            typingContext.addType(new TypingContext.TypeDefinition(name, fields, isTable, (Optional<List<String>>) visit(ctx.primaryKeyDefinition())));
        } else {
            typingContext.addType(new TypingContext.TypeDefinition(name, fields, isTable, Optional.empty()));
        }
        return null;
    }

    @Override
    public TypingContext.FieldDefinition visitColumnDefinition(RelationalParser.ColumnDefinitionContext ctx) {
        String fieldType = (ctx.columnType().customType == null) ?
                ctx.columnType().getText() :
                ParserUtils.safeCastLiteral(visit(ctx.columnType().customType), String.class);
        final String columnName = ParserUtils.safeCastLiteral(visit(ctx.colName), String.class);
        boolean isNullable = true;
        if (ctx.columnConstraint() != null) {
            isNullable = (boolean) visit(ctx.columnConstraint());
            if (!isNullable) {
                containsNonNullableArray = true;
            }
        }
        return new TypingContext.FieldDefinition(columnName, ParserUtils.toProtoType(fieldType), fieldType, ctx.ARRAY() != null, isNullable);
    }

    @Override
    public Boolean visitNullColumnConstraint(RelationalParser.NullColumnConstraintContext ctx) {
        return ctx.nullNotnull().NOT() == null;
    }

    @Override
    public Optional<List<String>> visitPrimaryKeyDefinition(RelationalParser.PrimaryKeyDefinitionContext ctx) {
        if (ctx.uid().size() == 0) {
            return Optional.empty();
        }
        return Optional.of(ctx.uid().stream().map(this::visit).map(f -> ParserUtils.safeCastLiteral(f, String.class)).collect(Collectors.toList()));
    }

    @Override
    public Void visitEnumDefinition(RelationalParser.EnumDefinitionContext ctx) {
        final var name = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
        final List<String> values = ctx.STRING_LITERAL().stream()
                .map(node -> ParserUtils.normalizeString(node.getText()))
                .collect(Collectors.toList());
        typingContext.addEnum(new TypingContext.EnumDefinition(name, values));
        return null;
    }

    @Override
    public Void visitIndexDefinition(RelationalParser.IndexDefinitionContext ctx) {
        final String indexName = ParserUtils.safeCastLiteral(ctx.indexName.accept(this), String.class);
        Assert.notNullUnchecked(indexName);
        final var viewPlan = (RelationalExpression) ctx.querySpecificationNointo().accept(this);
        Assert.thatUnchecked(viewPlan instanceof LogicalSortExpression);
        final var generator = KeyExpressionGenerator.from(viewPlan);
        final var indexExpressionAndType = generator.transform();
        typingContext.addIndex(generator.getRecordTypeName(), RecordMetaDataProto.Index.newBuilder().setRootExpression(NullableArrayUtils.wrapArray(indexExpressionAndType.getLeft().toKeyExpression(),
                typingContext.getTypeRepositoryBuilder().build().getMessageDescriptor(generator.getRecordTypeName()), containsNonNullableArray))
                .setName(indexName)
                .setType(indexExpressionAndType.getRight())
                .build(), List.of());
        return null;
    }

    @Override
    public ProceduralPlan visitDropDatabaseStatement(RelationalParser.DropDatabaseStatementContext ctx) {
        final String dbName = ParserUtils.safeCastLiteral(visit(ctx.path()), String.class);
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        return ProceduralPlan.of(constantActionFactory.getDropDatabaseConstantAction(URI.create(dbName), Options.NONE));
    }

    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(RelationalParser.DropSchemaTemplateStatementContext ctx) {
        final String id = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
        return ProceduralPlan.of(constantActionFactory.getDropSchemaTemplateConstantAction(Assert.notNullUnchecked(id), Options.NONE));
    }

    @Override
    public ProceduralPlan visitDropSchemaStatement(RelationalParser.DropSchemaStatementContext ctx) {
        final String schemaId = Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class));
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(schemaId);
        Assert.thatUnchecked(dbAndSchema.getLeft().isPresent(), String.format("invalid database identifier in '%s'", ctx.uid().getText()));
        return ProceduralPlan.of(constantActionFactory.getDropSchemaConstantAction(dbAndSchema.getLeft().get(), dbAndSchema.getRight(), Options.NONE));
    }

    /////// administration statements //////////////

    @Override
    public Object visitShowDatabasesStatement(RelationalParser.ShowDatabasesStatementContext ctx) {
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
