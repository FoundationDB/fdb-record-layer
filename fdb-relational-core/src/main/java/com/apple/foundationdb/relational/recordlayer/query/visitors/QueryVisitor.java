/*
 * QueryVisitor.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.DeleteExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.UpdateExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.EphemeralExpression;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperators;
import com.apple.foundationdb.relational.recordlayer.query.LogicalPlanFragment;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.ParseHelpers;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.StringTrieNode;
import com.apple.foundationdb.relational.recordlayer.util.MemoizedFunction;
import com.apple.foundationdb.relational.recordlayer.util.TypeUtils;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import org.antlr.v4.runtime.ParserRuleContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.relational.generated.RelationalParser.ALL;

@API(API.Status.EXPERIMENTAL)
public final class QueryVisitor extends DelegatingVisitor<BaseVisitor> {

    private QueryVisitor(@Nonnull BaseVisitor baseVisitor) {
        super(baseVisitor);
    }

    @Nonnull
    public static QueryVisitor of(@Nonnull BaseVisitor baseVisitor) {
        return new QueryVisitor(baseVisitor);
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitSelectStatement(@Nonnull RelationalParser.SelectStatementContext ctx) {
        final var logicalOperator = parseChild(ctx);
        return QueryPlan.LogicalQueryPlan.of(logicalOperator.getQuantifier().getRangesOver().get(), getDelegate().getPlanGenerationContext(), "TODO");
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx) {
        final var logicalOperator = parseChild(ctx);
        return QueryPlan.LogicalQueryPlan.of(logicalOperator.getQuantifier().getRangesOver().get(), getDelegate().getPlanGenerationContext(), "TODO");
    }

    @Nonnull
    @Override
    public LogicalOperator visitQuery(@Nonnull RelationalParser.QueryContext ctx) {
        if (ctx.ctes() != null) {
            final var currentPlanFragment = getDelegate().pushPlanFragment();
            visitCtes(ctx.ctes()).forEach(currentPlanFragment::addOperator);
            final var result = Assert.castUnchecked(ctx.queryExpressionBody().accept(this), LogicalOperator.class);
            getDelegate().popPlanFragment();
            return getDelegate().isTopLevel() ? LogicalOperator.generateSort(result, ImmutableList.of(), ImmutableSet.of(), Optional.empty()) : result;
        }
        return Assert.castUnchecked(ctx.queryExpressionBody().accept(this), LogicalOperator.class);
    }

    @Nonnull
    @Override
    public LogicalOperators visitCtes(@Nonnull RelationalParser.CtesContext ctx) {
        if (ctx.RECURSIVE() != null) {
            final RecursiveUnionExpression.TraversalStrategy traversalStrategy;
            if (ctx.traversalOrderClause() != null) {
                final var order = ctx.traversalOrderClause();
                if (order.LEVEL_ORDER() != null) {
                    traversalStrategy = RecursiveUnionExpression.TraversalStrategy.LEVEL;
                } else if (order.PRE_ORDER() != null) {
                    traversalStrategy = RecursiveUnionExpression.TraversalStrategy.PREORDER;
                } else if (order.POST_ORDER() != null) {
                    traversalStrategy = RecursiveUnionExpression.TraversalStrategy.POSTORDER;
                } else {
                    traversalStrategy = RecursiveUnionExpression.TraversalStrategy.ANY;
                    Assert.failUnchecked(ErrorCode.INTERNAL_ERROR, "Unsupported traversal " + order.getText());
                }
            } else {
                traversalStrategy = RecursiveUnionExpression.TraversalStrategy.ANY;
            }
            return LogicalOperators.of(ctx.namedQuery().stream().map(namedQuery -> handleRecursiveNamedQuery(namedQuery, traversalStrategy)).collect(ImmutableList.toImmutableList()));
        } else {
            Assert.thatUnchecked(ctx.traversalOrderClause() == null, ErrorCode.SYNTAX_ERROR, "traversal order clause can only be defined with recursive CTE");
        }
        return LogicalOperators.of(ctx.namedQuery().stream().map(this::visitNamedQuery).collect(ImmutableList.toImmutableList()));
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @Override
    public LogicalOperator visitNamedQuery(@Nonnull RelationalParser.NamedQueryContext ctx) {
        final var queryName = visitFullId(ctx.name);
        var logicalOperator = visitQuery(ctx.query());
        if (ctx.columnAliases != null) {
            final var columnAliases = visitFullIdList(ctx.columnAliases);
            SemanticAnalyzer.validateCteColumnAliases(logicalOperator, columnAliases);
            final var expressions = logicalOperator.getOutput().expanded();
            final var expressionsWithNewNames = Expressions.of(Streams.zip(expressions.stream(), columnAliases.stream(),
                    Expression::withName).collect(ImmutableList.toImmutableList()));
            logicalOperator = logicalOperator.withOutput(expressionsWithNewNames);
        }
        return logicalOperator.withName(queryName);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    public LogicalOperator handleRecursiveNamedQuery(@Nonnull final RelationalParser.NamedQueryContext recursiveQueryContext,
                                                     @Nonnull final RecursiveUnionExpression.TraversalStrategy traversalStrategy) {
        final var queryName = visitFullId(recursiveQueryContext.name);
        final Optional<Type> recursiveQueryType;
        final var memoized = MemoizedFunction.<ParserRuleContext, LogicalOperators>memoize(
                parserRuleContext -> {
                    final var result = parserRuleContext.accept(this);
                    if (result instanceof LogicalOperator) {
                        return LogicalOperators.ofSingle((LogicalOperator) result);
                    }
                    return Assert.castUnchecked(result, LogicalOperators.class);
                });
        {
            getDelegate().pushPlanFragment();
            recursiveQueryType = getDelegate().getSemanticAnalyzer().getRecursiveCteType(recursiveQueryContext.query(),
                    queryName, getDelegate()::visitFullId, memoized, this);
            getDelegate().popPlanFragment();
        }
        Assert.thatUnchecked(recursiveQueryType.isPresent(), ErrorCode.INVALID_RECURSION, "recursive CTE does not contain non-recursive term");
        final var type = recursiveQueryType.get();
        final var currentPlanFragment = getDelegate().pushPlanFragment();
        final var scanId = Identifier.of(queryName + "forScan");
        currentPlanFragment.addOperator(LogicalOperator.newTemporaryTableScan(queryName, scanId, type));
        final var partitions = getDelegate().getSemanticAnalyzer().partitionRecursiveQuery(recursiveQueryContext.query(),
                queryName, getDelegate()::visitFullId, memoized, this);
        final var nonRecursiveBranches = partitions.getLeft();
        final var recursiveBranches = partitions.getRight();
        getDelegate().popPlanFragment();
        if (recursiveBranches.isEmpty()) {
            return visitNamedQuery(recursiveQueryContext);
        }
        final var outerCorrelations = getDelegate().getCurrentPlanFragmentMaybe().map(LogicalPlanFragment::getOuterCorrelations).orElse(ImmutableSet.of());
        final var initialLeg = LogicalOperator.generateUnionAll(LogicalOperators.of(nonRecursiveBranches), outerCorrelations);
        final var recursiveLeg = LogicalOperator.generateUnionAll(LogicalOperators.of(recursiveBranches), outerCorrelations);
        final Identifier insertTempTableId = Identifier.of(queryName.getName() + "forInsert");
        final var initialLegInsert = LogicalOperator.newTemporaryTableInsert(initialLeg, insertTempTableId, type);
        final var recursiveLegInsert = LogicalOperator.newTemporaryTableInsert(recursiveLeg, insertTempTableId, type);
        final var recursiveUnion = new RecursiveUnionExpression(initialLegInsert.getQuantifier(), recursiveLegInsert.getQuantifier(),
                CorrelationIdentifier.of(scanId.getName()), CorrelationIdentifier.of(insertTempTableId.getName()), traversalStrategy);
        final var quantifier = Quantifier.forEach(Reference.initialOf(recursiveUnion));
        var logicalOperator = LogicalOperator.newNamedOperator(queryName, Expressions.fromQuantifier(quantifier), quantifier);
        if (recursiveQueryContext.columnAliases != null) {
            final var columnAliases = visitFullIdList(recursiveQueryContext.columnAliases);
            SemanticAnalyzer.validateCteColumnAliases(logicalOperator, columnAliases);
            final var expressions = logicalOperator.getOutput().expanded();
            final var expressionsWithNewNames = Expressions.of(Streams.zip(expressions.stream(), columnAliases.stream(),
                    Expression::withName).collect(ImmutableList.toImmutableList()));
            logicalOperator = logicalOperator.withOutput(expressionsWithNewNames);
        }
        return logicalOperator;
    }

    @Nonnull
    @Override
    public LogicalOperator visitSimpleTable(@Nonnull RelationalParser.SimpleTableContext simpleTableContext) {
        Assert.notNullUnchecked(simpleTableContext.fromClause(), ErrorCode.UNSUPPORTED_QUERY, "query is not supported");
        getDelegate().pushPlanFragment();
        simpleTableContext.fromClause().accept(this);

        var where = Optional.ofNullable(simpleTableContext.fromClause().whereExpr() == null ?
                null :
                visitWhereExpr(simpleTableContext.fromClause().whereExpr()));

        for (final var expression : getDelegate().getCurrentPlanFragment().getInnerJoinExpressions()) {
            where = where.map(e -> getDelegate().resolveFunction("and", e, expression)).or(() -> Optional.of(expression));
        }

        Expressions selectExpressions;
        List<OrderByExpression> orderBys = List.of();
        if (simpleTableContext.groupByClause() != null || hasAggregations(simpleTableContext.selectElements())) {
            var outerCorrelations = getDelegate().getCurrentPlanFragment().getOuterCorrelations();
            var selectWhere = LogicalOperator.generateSelectWhere(getDelegate().getLogicalOperators(), outerCorrelations, where, getDelegate().isForDdl());
            getDelegate().getCurrentPlanFragment().setOperator(selectWhere);
            final var groupByExpressions = simpleTableContext.groupByClause() == null ?
                    Expressions.empty() :
                    visitGroupByClause(simpleTableContext.groupByClause());

            final List<Expression> aliasedGroupByColumns = groupByExpressions.stream().filter(expression ->
                    expression instanceof EphemeralExpression).collect(ImmutableList.toImmutableList());
            if (!aliasedGroupByColumns.isEmpty()) {
                final var selectWhereWithExtraColumns = selectWhere.withAdditionalOutput(Expressions.of(aliasedGroupByColumns));
                getDelegate().getCurrentPlanFragment().setOperator(selectWhereWithExtraColumns);
                selectExpressions = visitSelectElements(simpleTableContext.selectElements());
                where = Optional.ofNullable(simpleTableContext.havingClause() == null ? null : visitHavingClause(simpleTableContext.havingClause()));
                getDelegate().getCurrentPlanFragment().setOperator(selectWhere);
            } else {
                selectExpressions = visitSelectElements(simpleTableContext.selectElements());
                where = Optional.ofNullable(simpleTableContext.havingClause() == null ? null : visitHavingClause(simpleTableContext.havingClause()));
            }
            outerCorrelations = getDelegate().getCurrentPlanFragment().getOuterCorrelations();
            final var literals = getDelegate().getPlanGenerationContext().getLiterals();
            final var groupBy = LogicalOperator.generateGroupBy(getDelegate().getLogicalOperators(), groupByExpressions,
                    selectExpressions, where, outerCorrelations, literals);
            if (groupByExpressions.isEmpty() && !getDelegate().isForDdl()) {
                selectExpressions = LogicalOperator.adjustCountOnEmpty(selectExpressions);
            }
            selectExpressions = selectExpressions.dereferenced(literals).expanded().pullUp(Expression.ofUnnamed(groupBy.getQuantifier().getRangesOver().get().getResultValue()).dereferenced(literals).getSingleItem().getUnderlyingValue(), groupBy.getQuantifier().getAlias(), outerCorrelations).clearQualifier();
            final var finalOuterCorrelation = outerCorrelations;
            where = where.map(predicate -> predicate.pullUp(groupBy.getQuantifier().getRangesOver().get().getResultValue(), groupBy.getQuantifier().getAlias(), finalOuterCorrelation));
            if (simpleTableContext.orderByClause() != null) {
                final var resolvedOrderBys = visitOrderByClauseForSelect(simpleTableContext.orderByClause(), selectExpressions);
                orderBys = OrderByExpression.pullUp(resolvedOrderBys.stream(),
                        groupBy.getQuantifier().getRangesOver().get().getResultValue(), groupBy.getQuantifier().getAlias(), outerCorrelations, Optional.empty())
                        .collect(ImmutableList.toImmutableList());
            }
            getDelegate().getCurrentPlanFragment().setOperator(groupBy);
        } else {
            selectExpressions = visitSelectElements(simpleTableContext.selectElements());
            if (simpleTableContext.orderByClause() != null) {
                orderBys = visitOrderByClauseForSelect(simpleTableContext.orderByClause(), selectExpressions);
            }
        }
        final var outerCorrelations = getDelegate().getCurrentPlanFragment().getOuterCorrelations();
        final var result = LogicalOperator.generateSelect(selectExpressions, getDelegate().getLogicalOperators(), where, orderBys,
                Optional.empty(), outerCorrelations, getDelegate().isTopLevel(), getDelegate().isForDdl());

        getDelegate().popPlanFragment();

        Assert.isNullUnchecked(simpleTableContext.limitClause(), ErrorCode.UNSUPPORTED_QUERY, "limit not yet supported in SQL");

        return result;
    }

    @Nonnull
    @Override
    public LogicalOperator visitParenthesisQuery(@Nonnull RelationalParser.ParenthesisQueryContext ctx) {
        return visitQuery(ctx.query());
    }

    @Nonnull
    @Override
    public LogicalOperator visitQueryTermDefault(@Nonnull RelationalParser.QueryTermDefaultContext queryTermDefaultContext) {
        return parseChild(queryTermDefaultContext);
    }

    @Nonnull
    @Override
    public LogicalOperator visitSetQuery(@Nonnull RelationalParser.SetQueryContext setQueryContext) {
        Assert.thatUnchecked(setQueryContext.quantifier != null && setQueryContext.quantifier.getType() == ALL,
                ErrorCode.UNSUPPORTED_QUERY, "only UNION ALL is supported");
        final var unionLegs = ImmutableList.of(Assert.castUnchecked(visit(setQueryContext.left), LogicalOperator.class),
                Assert.castUnchecked(visit(setQueryContext.right), LogicalOperator.class));
        return LogicalOperator.generateUnionAll(LogicalOperators.of(unionLegs), getDelegate().getCurrentPlanFragmentMaybe()
                .map(LogicalPlanFragment::getOuterCorrelations).orElse(ImmutableSet.of()));
    }

    @Nullable
    @Override
    public Void visitFromClause(@Nonnull RelationalParser.FromClauseContext fromClauseContext) {
        fromClauseContext.tableSources().accept(this);
        return null;
    }

    @Nullable
    @Override
    public Void visitTableSources(@Nonnull RelationalParser.TableSourcesContext ctx) {
        for (final var tableSource : ctx.tableSource()) {
            tableSource.accept(this);
        }
        return null;
    }

    @Nullable
    @Override
    public Void visitTableSourceBase(@Nonnull RelationalParser.TableSourceBaseContext ctx) {
        getDelegate().getCurrentPlanFragment().addOperator(Assert.castUnchecked(ctx.tableSourceItem().accept(this), LogicalOperator.class));
        for (final var joinPart : ctx.joinPart()) {
            joinPart.accept(this);
        }
        return null;
    }

    @Nullable
    @Override
    public Void visitInnerJoin(@Nonnull RelationalParser.InnerJoinContext ctx) {
        Assert.isNullUnchecked(ctx.uidList(), ErrorCode.UNSUPPORTED_QUERY, "using is not yet supported for inner join");
        getDelegate().getCurrentPlanFragment().addOperator(Assert.castUnchecked(ctx.tableSourceItem().accept(this), LogicalOperator.class));
        getDelegate().getCurrentPlanFragment().addInnerJoinExpression(Assert.castUnchecked(ctx.expression().accept(this), Expression.class));
        return null;
    }

    @Nonnull
    @Override
    public LogicalOperator visitAtomTableItem(@Nonnull RelationalParser.AtomTableItemContext atomTableItemContext) {
        final var tableIdentifier = Assert.castUnchecked(atomTableItemContext.tableName().accept(this), Identifier.class);
        final var tableAlias = Optional.of(atomTableItemContext.alias == null ? visitTableName(atomTableItemContext.tableName())
                                                                              : visitUid(atomTableItemContext.alias));
        final var requestedIndexes = atomTableItemContext.indexHint()
                .stream().flatMap(indexHint -> visitIndexHint(indexHint).stream()).collect(ImmutableSet.toImmutableSet());
        return LogicalOperator.generateAccess(tableIdentifier, tableAlias, requestedIndexes, getDelegate().getSemanticAnalyzer(),
                getDelegate().getCurrentPlanFragment(), getDelegate().getLogicalOperatorCatalog());
    }

    @Nonnull
    @Override
    public LogicalOperator visitSubqueryTableItem(@Nonnull RelationalParser.SubqueryTableItemContext subqueryTableItemContext) {
        final var alias = Assert.castUnchecked(subqueryTableItemContext.alias.accept(this), Identifier.class);
        final var selectOperator = visitQuery(subqueryTableItemContext.query());
        return selectOperator.withName(alias);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInlineTableItem(@Nonnull RelationalParser.InlineTableItemContext inlineTableItemContext) {
        NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> typeMaybe = null;
        if (inlineTableItemContext.inlineTableDefinition() != null) {
            typeMaybe = visitInlineTableDefinition(inlineTableItemContext.inlineTableDefinition());
            Assert.thatUnchecked(!inlineTableItemContext.recordConstructorForInlineTable().isEmpty());
            Type type = null;
            for (final var inlineTableContext : inlineTableItemContext.recordConstructorForInlineTable()) {
                final var rowExpression = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->  visitRecordConstructorForInlineTable(inlineTableContext));
                type = type == null ? rowExpression.getUnderlyingValue().getResultType()
                        : Type.maximumType(type, rowExpression.getUnderlyingValue().getResultType());
            }
            final var actualInlineTableType = type;
            final var inlineTypedWithNames = TypeUtils.setFieldNames(actualInlineTableType, typeMaybe.getRight());
            Assert.thatUnchecked(inlineTypedWithNames.isRecord());
            final var stateBuilder = LogicalPlanFragment.State.newBuilder().withTargetType(inlineTypedWithNames);
            getDelegate().getCurrentPlanFragment().setState(stateBuilder.build());
        }
        final ImmutableList.Builder<Expression> rowExpressionBuilder = ImmutableList.builder();
        for (final var inlineTableContext : inlineTableItemContext.recordConstructorForInlineTable()) {
            final var rowExpression = visitRecordConstructorForInlineTable(inlineTableContext);

            rowExpressionBuilder.add(rowExpression);
        }
        final var arguments = Expressions.of(rowExpressionBuilder.build()).asList().toArray(new Expression[0]);
        final var arrayOfTuples = getDelegate().resolveFunction("__internal_array", false, arguments);
        final var explodeExpression = new ExplodeExpression(arrayOfTuples.getUnderlyingValue());
        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(explodeExpression));
        var output = Expressions.of(LogicalOperator.convertToExpressions(resultingQuantifier));
        return typeMaybe == null
               ? LogicalOperator.newUnnamedOperator(output, resultingQuantifier)
               : LogicalOperator.newNamedOperator(Identifier.of(typeMaybe.getLeft()), output, resultingQuantifier);
    }

    @Override
    public LogicalOperator visitTableValuedFunction(@Nonnull RelationalParser.TableValuedFunctionContext tableValuedFunctionContext) {
        final var logicalOperator = visitTableFunction(tableValuedFunctionContext.tableFunction());
        final var aliasMaybe = Optional.ofNullable(tableValuedFunctionContext.uid() == null ? null :
                                                   visitUid(tableValuedFunctionContext.uid()));
        return aliasMaybe.map(logicalOperator::withName).orElse(logicalOperator);
    }

    @Nonnull
    @Override
    public Set<String> visitIndexHint(@Nonnull RelationalParser.IndexHintContext indexHintContext) {
        // currently only support USE INDEX '(' uidList ')' syntax
        Assert.isNullUnchecked(indexHintContext.IGNORE(), "index hint 'ignore' semantics not supported");
        Assert.isNullUnchecked(indexHintContext.FORCE(), "index hint 'force' semantics not supported");
        Assert.isNullUnchecked(indexHintContext.KEY(), "index hint 'key' not supported");
        Assert.isNullUnchecked(indexHintContext.FOR(), "index hint 'for' not supported");

        return indexHintContext.uidList().uid().stream().map(this::visitUid).map(Identifier::getName).collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatement(@Nonnull RelationalParser.InsertStatementContext ctx) {
        final var table = visitTableName(ctx.tableName());
        final var tableType = getDelegate().getSemanticAnalyzer().getTable(table);
        final var targetType = Assert.castUnchecked(tableType, RecordLayerTable.class).getType();
        getDelegate().pushPlanFragment();
        // TODO (Refactor insert parse rules)
        // (yhatem) leave it like this until the old plan generator is removed.
        final var lookahead = ctx.insertStatementValue().start.getType();
        final var isInsertFromSelect = lookahead == RelationalLexer.SELECT;
        Assert.thatUnchecked(!isInsertFromSelect || ctx.columns == null, ErrorCode.UNSUPPORTED_QUERY,
                "setting column ordering for insert with select is not supported");

        final LogicalOperator insertSource;
        if (isInsertFromSelect) {
            insertSource = Assert.castUnchecked(ctx.insertStatementValue().accept(this), LogicalOperator.class);
        } else {
            final var stateBuilder = LogicalPlanFragment.State.newBuilder().withTargetType(targetType);
            if (ctx.columns != null) {
                stateBuilder.withTargetTypeReorderings(toString(visitUidListWithNestingsInParens(ctx.columns)));
            }
            getDelegate().getCurrentPlanFragment().setState(stateBuilder.build());
            insertSource = Assert.castUnchecked(ctx.insertStatementValue().accept(this), LogicalOperator.class);
        }
        final var resultingInsert = LogicalOperator.generateInsert(insertSource, tableType);
        getDelegate().popPlanFragment();
        return resultingInsert;
    }

    @Nonnull
    private static StringTrieNode toString(@Nonnull CompatibleTypeEvolutionPredicate.FieldAccessTrieNode fieldAccessTrieNode) {
        if (fieldAccessTrieNode.getChildrenMap() == null) {
            return StringTrieNode.leafNode();
        }
        final var map = fieldAccessTrieNode.getChildrenMap().entrySet().stream().collect(ImmutableMap.toImmutableMap(pair -> pair.getKey().getName(), pair -> toString(pair.getValue())));
        return new StringTrieNode(map);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatementValueSelect(@Nonnull RelationalParser.InsertStatementValueSelectContext ctx) {
        return Assert.castUnchecked(ctx.queryExpressionBody().accept(this), LogicalOperator.class);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatementValueValues(@Nonnull RelationalParser.InsertStatementValueValuesContext ctx) {
        final ImmutableList.Builder<Expression> insertTuples = ImmutableList.builder();
        for (final var tupleContext : ctx.recordConstructorForInsert()) {
            insertTuples.add(visitRecordConstructorForInsert(tupleContext));
        }
        final var arguments = Expressions.of(insertTuples.build()).asList().toArray(new Expression[0]);
        final var arrayOfTuples = getDelegate().resolveFunction("__internal_array", false, arguments);
        final var explodeExpression = new ExplodeExpression(arrayOfTuples.getUnderlyingValue());
        final var resultingQuantifier = Quantifier.forEach(Reference.initialOf(explodeExpression));
        return LogicalOperator.newUnnamedOperator(Expressions.ofSingle(arrayOfTuples), resultingQuantifier);
    }

    @Nonnull
    @Override
    public LogicalOperator visitUpdateStatement(@Nonnull RelationalParser.UpdateStatementContext ctx) {
        final Identifier tableId = visitFullId(ctx.tableName().fullId());
        final SemanticAnalyzer semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        final RecordLayerTable table = Assert.castUnchecked(semanticAnalyzer.getTable(tableId), RecordLayerTable.class);
        final Type.Record tableType = table.getType();
        final LogicalOperator tableAccess = getDelegate().getLogicalOperatorCatalog().lookupTableAccess(tableId, semanticAnalyzer);

        getDelegate().pushPlanFragment().setOperator(tableAccess);
        // Note: doing an expansion here means that we don't have access to the pseudo-columns during the update
        // (and wouldn't have access to the invisible columns, see: https://github.com/FoundationDB/fdb-record-layer/pull/3787)
        // That also means that the target type of the update expression needs to match
        final var output = Expressions.ofSingle(semanticAnalyzer.expandStar(Optional.empty(), getDelegate().getLogicalOperators()));

        Optional<Expression> whereMaybe = ctx.whereExpr() == null ? Optional.empty() : Optional.of(visitWhereExpr(ctx.whereExpr()));
        final var updateSource = LogicalOperator.generateSimpleSelect(output, getDelegate().getLogicalOperators(), whereMaybe, Optional.of(tableId), ImmutableSet.of(), false);

        getDelegate().getCurrentPlanFragment().setOperator(updateSource);
        final ImmutableMap.Builder<FieldValue.FieldPath, Value> transformMapBuilder = ImmutableMap.builder();
        for (final RelationalParser.UpdatedElementContext updatedElementCtx : ctx.updatedElement()) {
            final List<Expression> targetAndUpdateExpressions = visitUpdatedElement(updatedElementCtx).asList();
            final FieldValue.FieldPath target = Assert.castUnchecked(targetAndUpdateExpressions.get(0).getUnderlyingValue(), FieldValue.class).getFieldPath();
            final Value update = targetAndUpdateExpressions.get(1).getUnderlyingValue();
            transformMapBuilder.put(target, update);
        }

        final var updateExpression = new UpdateExpression(Assert.castUnchecked(updateSource.getQuantifier(), Quantifier.ForEach.class),
                Assert.notNullUnchecked(tableType.getStorageName(), "Update target type must have storage type name available"),
                Type.Record.fromFields(tableType.getFields()), // Remove the type name from the update target type to avoid clashes with the table type in the update source
                transformMapBuilder.build());
        final var updateQuantifier = Quantifier.forEach(Reference.initialOf(updateExpression));
        final var resultingUpdate = LogicalOperator.newUnnamedOperator(Expressions.fromQuantifier(updateQuantifier), updateQuantifier);

        getDelegate().getCurrentPlanFragment().setOperator(resultingUpdate);

        //        if (ctx.CONTINUATION() != null) {
        //            getDelegate().getPlanGenerationContext().setContinuation((byte[]) visit(ctx.continuationAtom()));
        //        }

        if (ctx.RETURNING() != null) {
            final var selectExpressions = visitSelectElements(ctx.selectElements());
            final var result = LogicalOperator.generateSelect(selectExpressions, getDelegate().getLogicalOperators(),
                    Optional.empty(), List.of(), Optional.empty(),
                    getDelegate().getCurrentPlanFragment().getOuterCorrelations(), getDelegate().isTopLevel(), false);
            getDelegate().getCurrentPlanFragment().setOperator(result);
            return result;
        }
        final var result = LogicalOperator.generateSort(resultingUpdate, List.of(), Set.of(), Optional.empty());
        getDelegate().popPlanFragment();
        return result;
    }

    @Nonnull
    @Override
    public LogicalOperator visitDeleteStatement(@Nonnull RelationalParser.DeleteStatementContext ctx) {
        Assert.thatUnchecked(ctx.limitClause() == null, "limit is not supported");
        final Identifier tableId = visitFullId(ctx.tableName().fullId());
        final SemanticAnalyzer semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        final RecordLayerTable table = Assert.castUnchecked(semanticAnalyzer.getTable(tableId), RecordLayerTable.class);
        final LogicalOperator tableAccess = getDelegate().getLogicalOperatorCatalog().lookupTableAccess(tableId, semanticAnalyzer);

        getDelegate().pushPlanFragment().setOperator(tableAccess);
        final var output = Expressions.ofSingle(semanticAnalyzer.expandStar(Optional.empty(), getDelegate().getLogicalOperators()));

        Optional<Expression> whereMaybe = ctx.whereExpr() == null ? Optional.empty() : Optional.of(visitWhereExpr(ctx.whereExpr()));
        final var deleteSource = LogicalOperator.generateSimpleSelect(output, getDelegate().getLogicalOperators(), whereMaybe, Optional.of(tableId), ImmutableSet.of(), false);

        final var deleteExpression = new DeleteExpression(Assert.castUnchecked(deleteSource.getQuantifier(), Quantifier.ForEach.class), table.getType().getStorageName());
        final var deleteQuantifier = Quantifier.forEach(Reference.initialOf(deleteExpression));
        final var resultingDelete = LogicalOperator.newUnnamedOperator(Expressions.fromQuantifier(deleteQuantifier), deleteQuantifier);

        getDelegate().getCurrentPlanFragment().setOperator(resultingDelete);

        if (ctx.RETURNING() != null) {
            final var selectExpressions = visitSelectElements(ctx.selectElements());
            final var result = LogicalOperator.generateSelect(selectExpressions, getDelegate().getLogicalOperators(),
                    Optional.empty(), List.of(), Optional.empty(),
                    getDelegate().getCurrentPlanFragment().getOuterCorrelations(), getDelegate().isTopLevel(), false);
            getDelegate().getCurrentPlanFragment().setOperator(result);
            return result;
        }

        final var result = LogicalOperator.generateSort(resultingDelete, List.of(), Set.of(), Optional.empty());
        getDelegate().popPlanFragment();
        return result;
    }

    @Nonnull
    @Override
    public Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx) {
        // TODO (Rethink how execute continuation works)
        throw Assert.failUnchecked("execute package should not be handled here");
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx) {
        getDelegate().getPlanGenerationContext().setForExplain(ctx.EXPLAIN() != null);
        final var logicalOperator = Assert.castUnchecked(ctx.describeObjectClause().accept(this), LogicalOperator.class);
        return QueryPlan.LogicalQueryPlan.of(logicalOperator.getQuantifier().getRangesOver().get(), getDelegate().getPlanGenerationContext(), "TODO");
    }

    @Nonnull
    @Override
    public LogicalOperator visitDescribeStatements(@Nonnull RelationalParser.DescribeStatementsContext ctx) {
        return parseChild(ctx);
    }

    @Nonnull
    @Override
    public Object visitDescribeConnection(@Nonnull RelationalParser.DescribeConnectionContext ctx) {
        throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_QUERY, "query is not supported");
    }

    @Nonnull
    private LogicalOperator parseChild(ParserRuleContext context) {
        return Assert.castUnchecked(visitChildren(context), LogicalOperator.class);
    }

    @Nonnull
    public List<OrderByExpression> visitOrderByClauseForSelect(@Nonnull RelationalParser.OrderByClauseContext orderByClauseContext,
                                                               @Nonnull Expressions visibleSelectAliases) {
        final var validSelectAliases = Expressions.of(visibleSelectAliases.stream()
                .filter(expr -> expr.getName().isPresent() && !expr.getName().get().isQualified())
                .collect(ImmutableList.toImmutableList()));
        if (validSelectAliases.isEmpty()) {
            return visitOrderByClause(orderByClauseContext);
        }
        if (!getDelegate().isTopLevel()) {
            Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "order by is not supported in subquery");
        }
        final ImmutableList.Builder<OrderByExpression> orderBysBuilder = ImmutableList.builder();
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        for (final var orderByExpression : orderByClauseContext.orderByExpression()) {
            final var isAliasMaybe = isAliasMaybe(orderByExpression);
            final var matchingExpressionMaybe = isAliasMaybe.flatMap(alias -> semanticAnalyzer.lookupAlias(visitFullId(alias), validSelectAliases));
            matchingExpressionMaybe.ifPresentOrElse(
                    matchingExpression -> {
                        final var descending = ParseHelpers.isDescending(orderByExpression.orderClause());
                        final var nullsLast = ParseHelpers.isNullsLast(orderByExpression.orderClause(), descending);
                        orderBysBuilder.add(OrderByExpression.of(matchingExpression, descending, nullsLast));
                    },
                    () -> orderBysBuilder.add(visitOrderByExpression(orderByExpression))
            );
        }
        final var orderBys = orderBysBuilder.build();
        getDelegate().getSemanticAnalyzer().validateOrderByColumns(orderBys);
        return orderBys;
    }

    private boolean hasAggregations(@Nonnull RelationalParser.SelectElementsContext selectElementsContext) {
        return getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(
                () -> Streams.stream(visitSelectElements(selectElementsContext))
                        .anyMatch(expression -> !Iterables.isEmpty(Expression.Utils.filterUnderlyingAggregates(expression)))
        );
    }

    @Nonnull
    private static Optional<RelationalParser.FullIdContext> isAliasMaybe(@Nonnull RelationalParser.OrderByExpressionContext orderByExpressionContext) {
        if (!(orderByExpressionContext.expression() instanceof RelationalParser.PredicatedExpressionContext)) {
            return Optional.empty();
        }
        final var predicatedExpression = (RelationalParser.PredicatedExpressionContext) orderByExpressionContext.expression();
        if (predicatedExpression.predicate() != null) {
            return Optional.empty();
        }
        final var atomExpression = predicatedExpression.expressionAtom();
        if (!(atomExpression instanceof RelationalParser.FullColumnNameExpressionAtomContext)) {
            return Optional.empty();
        }
        final var fullColumnNameContext = (RelationalParser.FullColumnNameExpressionAtomContext) atomExpression;
        return Optional.of(fullColumnNameContext.fullColumnName().fullId());
    }
}
