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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.NotValue;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ExistsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.ddl.ConstantActionFactory;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.common.collect.Sets;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

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
            Assert.notNullUnchecked(continuationStr, "continuation can not be null");
            return QueryPlan.QpQueryplan.of(result, query, Base64.getDecoder().decode(continuationStr));
        } else {
            return QueryPlan.QpQueryplan.of(result, query);
        }
    }

    @Override
    public QueryPlan visitExplainStatement(RelationalParser.ExplainStatementContext ctx) {
        Assert.notNullUnchecked(ctx.selectStatement(), UNSUPPORTED_QUERY);
        RelationalExpression result = (RelationalExpression) ctx.selectStatement().accept(this);
        Assert.thatUnchecked(query.stripLeading().toUpperCase(Locale.ROOT).startsWith("EXPLAIN"));
        return new QueryPlan.ExplainPlan(QueryPlan.QpQueryplan.of(result, query.stripLeading().substring(7)));
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
        Assert.isNullUnchecked(ctx.selectIntoExpression(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.groupByClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.havingClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.windowClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.orderByClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.limitClause(), UNSUPPORTED_QUERY);
        Assert.notNullUnchecked(ctx.fromClause(), UNSUPPORTED_QUERY);

        parserContext.pushScope();
        ctx.fromClause().accept(this); // includes checking predicates
        ctx.selectElements().accept(this); // potentially sets explicit result columns on top-level select expression.
        return parserContext.popScope().convertToSelectExpression();
    }

    @ExcludeFromJacocoGeneratedReport // not reachable for now, but planned.
    @Override
    public RelationalExpression visitQuerySpecificationNointo(RelationalParser.QuerySpecificationNointoContext ctx) {
        Assert.thatUnchecked(ctx.selectSpec().isEmpty(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.groupByClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.havingClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.windowClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.orderByClause(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.limitClause(), UNSUPPORTED_QUERY);
        Assert.notNullUnchecked(ctx.fromClause(), UNSUPPORTED_QUERY);

        parserContext.pushScope();
        ctx.fromClause().accept(this); // includes checking predicates
        ctx.selectElements().accept(this); // potentially sets explicit result columns on top-level select expression.
        return parserContext.popScope().convertToSelectExpression();
    }

    @Override
    public Void visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx) {
        if (ctx.star != null) {
            //            // optimizes the way we add a value as-is.
            //            final var numQun = parserContext.getCurrentScope().getAllQuantifiers().size();
            //            if(numQun == 1) {
            //                parserContext.getCurrentScope().addResultValue(parserContext.getCurrentScope().getAllQuantifiers().get(0).getFlowedObjectValue());
            //            } else {
            parserContext.getCurrentScope()
                    .getAllQuantifiers().stream().filter(qun -> qun instanceof Quantifier.ForEach)
                    .flatMap(qun -> qun.getFlowedColumns().stream())
                    .forEach(c -> parserContext.getCurrentScope().addProjectionColumn(c));
            //            }
        } else {
            ctx.selectElement().forEach(e -> e.accept(this));
        }
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Void visitSelectStarElement(RelationalParser.SelectStarElementContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Void visitSelectColumnElement(RelationalParser.SelectColumnElementContext ctx) {
        parserContext.getCurrentScope().addProjectionColumn((Column<? extends Value>) ctx.fullColumnName().accept(this));
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Void visitSelectFunctionElement(RelationalParser.SelectFunctionElementContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport // can not reach code path for now
    public Void visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext ctx) {
        Assert.isNullUnchecked(ctx.LOCAL_ID(), UNSUPPORTED_QUERY);
        final var expressionObj = ctx.expression().accept(this);
        Assert.thatUnchecked(expressionObj instanceof Value, UNSUPPORTED_QUERY);
        final var expression = (Value) expressionObj;
        if (ctx.AS() != null) {
            final var alias = ParserUtils.unquoteString(ParserUtils.safeCastLiteral(ctx.uid().accept(this), String.class));
            Assert.notNullUnchecked(alias, UNSUPPORTED_QUERY);
            parserContext.getCurrentScope().addProjectionColumn(Column.of(Type.Record.Field.of(expression.getResultType(), Optional.of(alias), Optional.empty()), expression));
        } else {
            parserContext.getCurrentScope().addProjectionColumn(Column.unnamedOf(expression));
        }
        return null;
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
            final Collection<QuantifiedValue> aliases = parserContext.getCurrentScope().getAllQuantifiers().stream().filter(qun -> qun instanceof Quantifier.ForEach).flatMap(qun -> qun.getFlowedValues().stream()).collect(Collectors.toList()); // not sure this is correct
            Assert.thatUnchecked(!aliases.isEmpty());
            final Optional<QueryPredicate> predicateOptional = ((BooleanValue) predicate).toQueryPredicate(aliases.stream().findFirst().get().getAlias());
            Assert.thatUnchecked(predicateOptional.isPresent(), "query is not supported");
            parserContext.getCurrentScope().setPredicate(predicateOptional.get()); // improve
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

    @ExcludeFromJacocoGeneratedReport // not reachable for now.
    @Override
    public Void visitTableSourceNested(RelationalParser.TableSourceNestedContext ctx) {
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
        Assert.thatUnchecked(Sets.difference(hintedIndexes, allIndexes).isEmpty(), String.format("Unknown index(es) %s", String.join(",", Sets.difference(hintedIndexes, allIndexes)),
                ErrorCode.SYNTAX_ERROR));
        Set<AccessHint> accessHintSet = hintedIndexes.stream().map(IndexAccessHint::new).collect(Collectors.toSet());

        final RelationalExpression from = ParserUtils.quantifyOver((QualifiedIdentifierValue) tableName, parserContext, new AccessHints(accessHintSet.toArray(AccessHint[]::new)));
        final var quantifierAlias = ctx.alias != null ?
                ParserUtils.unquoteString(ParserUtils.safeCastLiteral(visit(ctx.alias), String.class)) :
                ParserUtils.unquoteString(ParserUtils.safeCastLiteral(tableName, String.class));
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

        return ctx.uidList().uid().stream().map(RuleContext::getText).collect(Collectors.toSet());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Typed visitSubqueryTableItem(RelationalParser.SubqueryTableItemContext ctx) {
        final var subqueryAlias = ParserUtils.unquoteString(ctx.alias.getText());
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
            return (Value) (new AndOrValue.AndFn().encapsulate(parserContext, List.of(left, right)));
        }
        return (Value) (new AndOrValue.OrFn().encapsulate(parserContext, List.of(left, right)));
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
            return (Value) (new RelOpValue.NotEqualsFn().encapsulate(parserContext, List.of(left, right)));
        } else {
            return (Value) (new RelOpValue.EqualsFn().encapsulate(parserContext, List.of(left, right)));
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
            return new RelOpValue.NotNullFn().encapsulate(parserContext, List.of(left));
        } else {
            return new RelOpValue.IsNullFn().encapsulate(parserContext, List.of(left));
        }
    }

    @Override
    public Value visitBinaryComparisonPredicate(RelationalParser.BinaryComparisonPredicateContext ctx) {
        final Value left = (Value) (visit(ctx.left));
        final Value right = (Value) (visit(ctx.right));
        BuiltInFunction<Value> comparisonFunction = ParserUtils.getFunction(ctx.comparisonOperator().getText());
        return (Value) (comparisonFunction.encapsulate(parserContext, List.of(left, right)));
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

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitFunctionCallExpressionAtom(RelationalParser.FunctionCallExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
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
        final var expression = ctx.selectStatement().accept(this);
        Assert.thatUnchecked(expression instanceof RelationalExpression);
        RelationalExpression subquery = (RelationalExpression) expression;
        Assert.notNullUnchecked(subquery);
        final Quantifier.Existential existsQuantifier = Quantifier.existential(GroupExpressionRef.of(subquery));
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
        BuiltInFunction<Value> mathFunction = ParserUtils.getFunction(ctx.mathOperator().getText());
        return (Value) (mathFunction.encapsulate(parserContext, List.of(left, right)));
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitJsonExpressionAtom(RelationalParser.JsonExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    ////// DB Objects /////


    @Override
    public Typed visitFullId(RelationalParser.FullIdContext ctx) {
        Assert.thatUnchecked(!ctx.uid().isEmpty());
        final String firstPart = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
        Assert.notNullUnchecked(firstPart);
        if (ctx.DOT_ID() != null) {
            final String secondPart = ctx.DOT_ID().getText();
            Assert.notNullUnchecked(secondPart);
            return QualifiedIdentifierValue.of(firstPart, secondPart.substring(1));
        }
        return QualifiedIdentifierValue.of(firstPart);
    }

    @Override
    public Typed visitTableName(RelationalParser.TableNameContext ctx) {
        final Typed fullId = (Typed) ctx.fullId().accept(this);
        Assert.thatUnchecked(fullId instanceof QualifiedIdentifierValue);
        final QualifiedIdentifierValue qualifiedIdentifierValue = (QualifiedIdentifierValue) fullId;
        Assert.thatUnchecked(qualifiedIdentifierValue.getParts().length <= 2);
        return qualifiedIdentifierValue;
    }

    @Override
    public Value visitFullColumnNameExpressionAtom(RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        Column<? extends Value> column = (Column<? extends Value>) (ctx.fullColumnName().accept(this));
        return column.getValue();
    }

    @Override
    public Column<? extends Value> visitFullColumnName(RelationalParser.FullColumnNameContext ctx) {
        List<String> fieldParts = Stream.concat(
                Stream.of(Assert.notNullUnchecked(ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class))),
                ctx.dottedId().stream().map(p -> ParserUtils.safeCastLiteral(p.accept(this), String.class))
        ).collect(Collectors.toList());

        final Quantifier qun = ParserUtils.findFieldPath(fieldParts.get(0), parserContext);
        if (qun.getAlias().toString().equals(fieldParts.get(0))) {
            fieldParts = fieldParts.stream().skip(1).collect(Collectors.toList());
        }
        final FieldValue fieldValue = ParserUtils.getFieldValue(fieldParts, qun.getFlowedObjectValue());
        return Column.of(Type.Record.Field.of(fieldValue.getResultType(), Optional.of(fieldParts.get(fieldParts.size() - 1)), Optional.empty()), fieldValue);
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
    public Typed visitUid(RelationalParser.UidContext ctx) {
        if (ctx.simpleId() != null) {
            return LiteralValue.ofScalar(ParserUtils.safeCastLiteral(visit(ctx.simpleId()), String.class));
        } else {
            return LiteralValue.ofScalar(ParserUtils.unquoteString(ctx.getText()));
        }
    }

    @Override
    public Typed visitSimpleId(RelationalParser.SimpleIdContext ctx) {
        return LiteralValue.ofScalar(ParserUtils.unquoteString(ctx.getText()));
    }

    @Override
    public Typed visitDottedId(RelationalParser.DottedIdContext ctx) {
        if (ctx.DOT_ID() != null) {
            final var unquoted = ParserUtils.unquoteString(ctx.getText());
            Assert.notNullUnchecked(unquoted);
            return LiteralValue.ofScalar(ParserUtils.trimStartingDot(unquoted));
        }
        return LiteralValue.ofScalar(ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class));
    }

    //// Literals ////


    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Typed visitFileSizeLiteral(RelationalParser.FileSizeLiteralContext ctx) {
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
    public Typed visitNullLiteral(RelationalParser.NullLiteralContext ctx) {
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
        return new LiteralValue<>(ParserUtils.unquoteString(ctx.getText()));
    }

    @Override
    public Value visitDecimalLiteral(RelationalParser.DecimalLiteralContext ctx) {
        return ParserUtils.parseDecimal(ctx.getText(), false);
    }

    /////// DDL statements //////////////

    @Override
    public ProceduralPlan visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        Assert.thatUnchecked(ctx.createStatement() != null || ctx.dropStatement() != null, UNSUPPORTED_QUERY);
        return (ProceduralPlan) visitChildren(ctx);
    }

    @Override
    public ProceduralPlan visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx) {
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(ctx.schemaId().getText());
        final String templateId = ParserUtils.unquoteString(ctx.templateId().getText());
        return ProceduralPlan.of(constantActionFactory.getCreateSchemaConstantAction(dbAndSchema.getLeft().orElse(dbUri),
                dbAndSchema.getRight(), templateId, Options.NONE));
    }

    @Override
    public Void visitTemplateClause(RelationalParser.TemplateClauseContext ctx) {
        if (ctx.structOrTableDefinition() != null) {
            ctx.structOrTableDefinition().accept(this);
        } else if (ctx.indexDefinition() != null) {
            ctx.indexDefinition().accept(this);
        } else {
            typingContext.addAllToTypeRepository();
            // reset metadata such that we can use it to resolve identifiers in subsequent materialized view definition(s).
            parserContext = parserContext.withTypeRepositoryBuilder(typingContext.getTypeRepositoryBuilder())
                    .withScannableRecordTypes(typingContext.getTableNames(), typingContext.getFieldDescriptorMap())
                    .withIndexNames(typingContext.getIndexNames());
            ctx.indexAsSelectDefinition().accept(this);
        }
        return null;
    }

    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        final var schemaTemplateName = ParserUtils.unquoteString(ctx.schemaTemplateId().getText());

        // collect all tables, their indices, and custom types definitions.
        ctx.templateClause().forEach(s -> s.accept(this));
        typingContext.addAllToTypeRepository();
        // reset metadata such that we can use it to resolve identifiers in subsequent index definition(s).
        parserContext = parserContext.withTypeRepositoryBuilder(typingContext.getTypeRepositoryBuilder())
                .withScannableRecordTypes(typingContext.getTableNames(), typingContext.getFieldDescriptorMap())
                .withIndexNames(typingContext.getIndexNames());

        SchemaTemplate schemaTemplate = typingContext.generateSchemaTemplate(schemaTemplateName);
        return ProceduralPlan.of(constantActionFactory.getCreateSchemaTemplateConstantAction(schemaTemplate, Options.NONE));
    }

    @Override
    public ProceduralPlan visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
        final var dbName = ParserUtils.unquoteString(ctx.path().getText());
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        return ProceduralPlan.of(constantActionFactory.getCreateDatabaseConstantAction(URI.create(dbName), Options.NONE));
    }

    @Override
    public Void visitStructOrTableDefinition(RelationalParser.StructOrTableDefinitionContext ctx) {
        Assert.thatUnchecked(ctx.STRUCT() == null || ctx.primaryKeyDefinition() == null,
                String.format("Illegal struct definition '%s'", ctx.uid().getText()), ErrorCode.SYNTAX_ERROR);
        final var name = ctx.uid().getText();
        final List<TypingContext.FieldDefinition> fields = ctx.columnDefinition().stream().map(c ->
                (TypingContext.FieldDefinition) c.accept(this)).collect(Collectors.toList());
        final var isTable = ctx.STRUCT() == null;
        if (ctx.primaryKeyDefinition() != null) {
            typingContext.addType(new TypingContext.TypeDefinition(name, fields, isTable, Optional.of(((List<String>) ctx.primaryKeyDefinition().accept(this)))));
        } else {
            typingContext.addType(new TypingContext.TypeDefinition(name, fields, isTable, Optional.empty()));
        }
        return null;
    }

    @Override
    public TypingContext.FieldDefinition visitColumnDefinition(RelationalParser.ColumnDefinitionContext ctx) {
        final var fieldType = ParserUtils.toProtoType(ctx.columnType().getText());
        return new TypingContext.FieldDefinition(ctx.colName.getText(), fieldType, ctx.columnType().getText(), ctx.ARRAY() != null);
    }

    @Override
    public List<String> visitPrimaryKeyDefinition(RelationalParser.PrimaryKeyDefinitionContext ctx) {
        return ctx.uid().stream().map(RuleContext::getText).collect(Collectors.toList());
    }

    @Override
    public Void visitValueIndexDefinition(RelationalParser.ValueIndexDefinitionContext ctx) {
        final String tableName = ctx.tblName.getText();
        final String indexName = ctx.idxName.getText();
        final List<KeyExpression> fieldExpressions = ctx.idxField().stream().map(RuleContext::getText).map(Key.Expressions::field).collect(Collectors.toList());
        final List<KeyExpression> includeExpressions = ctx.incField().stream().map(RuleContext::getText).map(Key.Expressions::field).collect(Collectors.toList());
        fieldExpressions.add(0, Key.Expressions.recordType());
        KeyExpression rootExpression;
        if (includeExpressions.isEmpty()) {
            rootExpression = Key.Expressions.concat(fieldExpressions);
        } else {
            List<KeyExpression> allFields = new ArrayList<>(fieldExpressions.size() + includeExpressions.size());
            allFields.addAll(fieldExpressions);
            allFields.addAll(includeExpressions);
            rootExpression = Key.Expressions.keyWithValue(Key.Expressions.concat(allFields), fieldExpressions.size());
        }
        typingContext.addIndex(tableName, RecordMetaDataProto.Index.newBuilder().setRootExpression(rootExpression.toKeyExpression())
                .setName(indexName).setType("value").build(), Stream.concat(ctx.idxField().stream().map(RuleContext::getText), ctx.incField().stream().map(RuleContext::getText)).collect(Collectors.toList()));
        return null;
    }

    @Override
    public Void visitIndexAsSelectDefinition(RelationalParser.IndexAsSelectDefinitionContext ctx) {
        final String viewName = ctx.indexName.getText();
        final RelationalExpression viewPlan = (RelationalExpression) ctx.querySpecificationNointo().accept(this);
        final var result = PlanUtils.getMaterializedViewKeyDefinition(viewPlan);
        final KeyExpression indexExpression = result.getRight();
        typingContext.addIndex(result.getLeft(), RecordMetaDataProto.Index.newBuilder().setRootExpression(indexExpression.toKeyExpression())
                .setName(viewName).setType("value").build(), List.of());
        return null;
    }

    @Override
    public ProceduralPlan visitDropDatabaseStatement(RelationalParser.DropDatabaseStatementContext ctx) {
        final var dbName = ParserUtils.unquoteString(ctx.path().getText());
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        return ProceduralPlan.of(constantActionFactory.getDropDatabaseConstantAction(URI.create(dbName), Options.NONE));
    }

    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return ProceduralPlan.of(constantActionFactory.getDropSchemaTemplateConstantAction(ctx.uid().getText(), Options.NONE));
    }

    @Override
    public ProceduralPlan visitDropSchemaStatement(RelationalParser.DropSchemaStatementContext ctx) {
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(ctx.uid().getText());
        Assert.thatUnchecked(dbAndSchema.getLeft().isPresent(), String.format("invalid database identifier in '%s'", ctx.uid().getText()));
        return ProceduralPlan.of(constantActionFactory.getDropSchemaConstantAction(dbAndSchema.getLeft().get(), dbAndSchema.getRight(), Options.NONE));
    }

    /////// administration statements //////////////

    @Override
    public Object visitShowDatabasesStatement(RelationalParser.ShowDatabasesStatementContext ctx) {
        if (ctx.path() != null) {
            final var dbName = ParserUtils.unquoteString(ctx.path().getText());
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
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(ctx.schemaId().getText());
        return QueryPlan.MetadataQueryPlan.of(ddlQueryFactory.getDescribeSchemaQueryAction(dbAndSchema.getLeft().orElse(dbUri), dbAndSchema.getRight()));
    }

    @Override
    public QueryPlan visitSimpleDescribeSchemaTemplateStatement(RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        final var schemaTemplateName = ctx.uid().getText();
        return QueryPlan.MetadataQueryPlan.of(ddlQueryFactory.getDescribeSchemaTemplateQueryAction(schemaTemplateName));
    }

    @Nonnull
    public Set<String> getFilteredRecords() {
        return parserContext.getFilteredRecords();
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
        SyntaxErrorListener listener = new SyntaxErrorListener();
        parser.addErrorListener(listener);
        RelationalParser.RootContext rootContext = parser.root();
        if (!listener.getSyntaxErrors().isEmpty()) {
            throw listener.getSyntaxErrors().get(0).toRelationalException();
        }
        return rootContext;
    }
}
