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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.NotValue;
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
    private final ParserContext parserContext;

    @Nonnull
    private final GraphExpansion.Builder graphExpansionBuilder;

    // this field holds all the relations (record types) that we want to filter-scan from. Currently, it is needed
    // upfront by the planner, and it should become unnecessary later on.
    @Nonnull
    private final Set<String> filteredRecords;

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
    public AstVisitor(@Nonnull final ParserContext parserContext,
                      @Nonnull final String query,
                      @Nonnull final ConstantActionFactory constantActionFactory,
                      @Nonnull final DdlQueryFactory ddlQueryFactory,
                      @Nonnull final URI dbUri) {
        this.query = query;
        this.parserContext = parserContext;
        this.constantActionFactory = constantActionFactory;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
        this.graphExpansionBuilder = GraphExpansion.builder();
        this.filteredRecords = new LinkedHashSet<>();
        this.typingContext = TypingContext.create();
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        return nextResult != null ? nextResult : aggregate;
    }

    @Override
    public QueryPlan.QpQueryplan visitDmlStatement(RelationalParser.DmlStatementContext ctx) {
        Assert.notNullUnchecked(ctx.selectStatementWithContinuation(), UNSUPPORTED_QUERY);
        return (QueryPlan.QpQueryplan) ctx.selectStatementWithContinuation().accept(this);
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
    public QueryPlan.QpQueryplan visitSelectStatementWithContinuation(RelationalParser.SelectStatementWithContinuationContext ctx) {
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

        visit(ctx.fromClause()); // includes checking predicates
        visit(ctx.selectElements());

        return graphExpansionBuilder.build().seal().buildSelect();
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

        visit(ctx.fromClause()); // includes checking predicates
        visit(ctx.selectElements());

        return graphExpansionBuilder.build().seal().buildSelect();
    }

    @Override
    public Typed visitSelectElements(RelationalParser.SelectElementsContext ctx) {
        if (ctx.star != null) {
            graphExpansionBuilder.pullUpAllExistingQuantifiers();
        } else {
            ctx.selectElement().forEach(this::visit);
        }
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Typed visitSelectStarElement(RelationalParser.SelectStarElementContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    public Typed visitSelectColumnElement(RelationalParser.SelectColumnElementContext ctx) {
        Assert.isNullUnchecked(ctx.AS(), UNSUPPORTED_QUERY); // don't know how to handle column aliases with current API yet.
        graphExpansionBuilder.addResultValue((Value) (visit(ctx.fullColumnName())));
        return null;
    }

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Typed visitSelectFunctionElement(RelationalParser.SelectFunctionElementContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport // can not reach code path for now
    public Typed visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext ctx) {
        Assert.isNullUnchecked(ctx.LOCAL_ID(), UNSUPPORTED_QUERY);
        Assert.isNullUnchecked(ctx.AS(), UNSUPPORTED_QUERY); // don't know how to handle column aliases with current API yet.
        graphExpansionBuilder.addResultValue((Value) (visit(ctx.expression())));
        return null;
    }

    @Override
    public Typed visitFromClause(RelationalParser.FromClauseContext ctx) {
        Assert.notNullUnchecked(ctx.FROM(), UNSUPPORTED_QUERY);
        // prepare parser context for resolving aliases by parsing FROM clause first.
        visit(ctx.tableSources());
        if (ctx.WHERE() != null) {
            final Value predicate = (Value) (visit(ctx.whereExpr));
            Assert.notNullUnchecked(predicate);
            Assert.thatUnchecked(predicate instanceof BooleanValue, String.format("unexpected predicate of type %s", predicate.getClass().getSimpleName()));
            final Collection<QuantifiedValue> aliases = parserContext.getCurrentScope().getBoundQuantifiers().values();
            Assert.thatUnchecked(!aliases.isEmpty());
            final Optional<QueryPredicate> predicateOptional = ((BooleanValue) predicate).toQueryPredicate(aliases.stream().findFirst().get().getAlias());
            Assert.thatUnchecked(predicateOptional.isPresent(), "query is not supported");
            graphExpansionBuilder.addPredicate(predicateOptional.get());
        }
        return null;
    }

    @Override
    public RelationalExpression visitTableSources(RelationalParser.TableSourcesContext ctx) {
        Assert.thatUnchecked(ctx.tableSource().size() == 1, UNSUPPORTED_QUERY);
        return (RelationalExpression) (visit(ctx.tableSource(0)));
    }

    @Override
    public RelationalExpression visitTableSourceBase(RelationalParser.TableSourceBaseContext ctx) {
        Assert.thatUnchecked(ctx.joinPart().isEmpty(), UNSUPPORTED_QUERY);
        return (RelationalExpression) (visit(ctx.tableSourceItem()));
    }

    @ExcludeFromJacocoGeneratedReport // not reachable for now.
    @Override
    public RelationalExpression visitTableSourceNested(RelationalParser.TableSourceNestedContext ctx) {
        Assert.thatUnchecked(ctx.joinPart().isEmpty(), UNSUPPORTED_QUERY);
        return (RelationalExpression) (visit(ctx.tableSourceItem()));
    }

    @Override
    public Typed visitAtomTableItem(RelationalParser.AtomTableItemContext ctx) {
        Assert.isNullUnchecked(ctx.PARTITION(), UNSUPPORTED_QUERY);
        final String recordTypeName = ParserUtils.safeCastLiteral(visit(ctx.tableName()), String.class);
        Assert.notNullUnchecked(recordTypeName);
        final ImmutableSet<String> recordTypeNameSet = ImmutableSet.<String>builder().add(recordTypeName).build();
        final RecordMetaData recordMetaData = parserContext.getRecordMetaData();
        final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap = recordMetaData.getFieldDescriptorMapFromNames(recordTypeNameSet);
        final Set<String> allAvailableRecordTypes = recordMetaData.getRecordTypes().keySet();
        final Set<String> allIndexes = recordMetaData.getAllIndexes().stream().map(Index::getName).collect(Collectors.toSet());
        // get index hints
        final Set<String> hintedIndexes = new HashSet<>();
        for (final RelationalParser.IndexHintContext indexHintContext : ctx.indexHint()) {
            hintedIndexes.addAll(visitIndexHint(indexHintContext));
        }
        // check if all hinted indexes exist
        Assert.thatUnchecked(Sets.difference(hintedIndexes, allIndexes).isEmpty(), String.format("Unknown index(es) %s", String.join(",", Sets.difference(hintedIndexes, allIndexes)),
                ErrorCode.SYNTAX_ERROR));
        Set<AccessHint> accessHintSet = hintedIndexes.stream().map(IndexAccessHint::new).collect(Collectors.toSet());
        RelationalExpression typeFilterExpression = new LogicalTypeFilterExpression(recordTypeNameSet,
                Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(allAvailableRecordTypes, new AccessHints(accessHintSet.toArray(AccessHint[]::new))))), // todo index hints.
                Type.Record.fromFieldDescriptorsMap(fieldDescriptorMap));

        // improve
        final Quantifier.ForEach forEachQuantifier = Quantifier.forEachBuilder().build(GroupExpressionRef.of(typeFilterExpression));
        final QuantifiedObjectValue quantifiedObjectValue = forEachQuantifier.getFlowedObjectValue();
        graphExpansionBuilder.addQuantifier(forEachQuantifier);
        final String alias = (ctx.AS() != null) ? ParserUtils.safeCastLiteral(visit(ctx.alias), String.class) : recordTypeName;
        Assert.notNullUnchecked(alias);
        parserContext.pushScope(Map.of(CorrelationIdentifier.of(alias), quantifiedObjectValue));
        filteredRecords.addAll(recordTypeNameSet);
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

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Typed visitSubqueryTableItem(RelationalParser.SubqueryTableItemContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
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

    @Override // not supported yet
    @ExcludeFromJacocoGeneratedReport
    public Value visitExistsExpressionAtom(RelationalParser.ExistsExpressionAtomContext ctx) {
        Assert.failUnchecked(UNSUPPORTED_QUERY);
        return null;
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
        BuiltInFunction<Value> comparisonFunction = ParserUtils.getFunction(ctx.mathOperator().getText());
        return (Value) (comparisonFunction.encapsulate(parserContext, List.of(left, right)));
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
        final StringBuilder result = new StringBuilder();
        result.append(ParserUtils.safeCastLiteral(visit(ctx.uid(0)), String.class));
        if (ctx.DOT_ID() != null) {
            result.append(ParserUtils.unquoteString(ctx.DOT_ID().getText()));
        } else {
            Assert.thatUnchecked(!ctx.uid().isEmpty());
            ctx.uid().stream().skip(1).forEach(d -> result.append(ParserUtils.safeCastLiteral(visit(d), String.class)));
        }
        return LiteralValue.ofScalar(result.toString());
    }

    @Override
    public Typed visitTableName(RelationalParser.TableNameContext ctx) {
        return (Typed) ctx.fullId().accept(this);
    }

    @Override
    public Value visitFullColumnName(RelationalParser.FullColumnNameContext ctx) {
        if (!ctx.dottedId().isEmpty()) {
            Assert.thatUnchecked(ctx.dottedId().size() == 1, UNSUPPORTED_QUERY); // todo nesting
            final String recordTypeName = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
            Assert.notNullUnchecked(recordTypeName);
            final String fieldName = ParserUtils.safeCastLiteral(visit(ctx.dottedId(0)), String.class);
            Assert.notNullUnchecked(fieldName);
            Assert.thatUnchecked(fieldName.length() > 1);
            final FieldValue fieldValue = ParserUtils.getFieldValue(fieldName.substring(1), parserContext.resolveIdentifier(recordTypeName));
            Assert.notNullUnchecked(fieldValue, String.format("attempting to query non existing field %s%s", recordTypeName, fieldName));
            return fieldValue;
        } else {
            final String fieldName = ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class);
            Assert.notNullUnchecked(fieldName);
            final FieldValue fieldValue = ParserUtils.getFieldValue(fieldName, parserContext);
            Assert.notNullUnchecked(fieldValue, String.format("attempting to query non existing field %s", fieldName));
            return fieldValue;
        }
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
            return LiteralValue.ofScalar(ParserUtils.unquoteString(ctx.getText()));
        }
        return LiteralValue.ofScalar("." + ParserUtils.safeCastLiteral(visit(ctx.uid()), String.class));
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
                dbAndSchema.getRight(), templateId, Options.create()));
    }

    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        final var schemaTemplateName = ParserUtils.unquoteString(ctx.schemaTemplateId().getText());
        // collect all tables, their indices, and custom types definitions.
        ctx.structOrTableDefinition().forEach(s -> s.accept(this));
        ctx.indexDefinition().forEach(s -> s.accept(this));
        typingContext.addAllToTypeRepository();
        SchemaTemplate schemaTemplate = typingContext.generateSchemaTemplate(schemaTemplateName);
        return ProceduralPlan.of(constantActionFactory.getCreateSchemaTemplateConstantAction(schemaTemplate, Options.create()));
    }

    @Override
    public ProceduralPlan visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
        final var dbName = ParserUtils.unquoteString(ctx.path().getText());
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        return ProceduralPlan.of(constantActionFactory.getCreateDatabaseConstantAction(URI.create(dbName), Options.create()));
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
    public ProceduralPlan visitDropDatabaseStatement(RelationalParser.DropDatabaseStatementContext ctx) {
        final var dbName = ParserUtils.unquoteString(ctx.path().getText());
        Assert.notNullUnchecked(dbName);
        Assert.thatUnchecked(ParserUtils.isProperDbUri(dbName), String.format("invalid database path '%s'", ctx.path().getText()), ErrorCode.INVALID_PATH);
        return ProceduralPlan.of(constantActionFactory.getDropDatabaseConstantAction(URI.create(dbName), Options.create()));
    }

    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return ProceduralPlan.of(constantActionFactory.getDropSchemaTemplateConstantAction(ctx.uid().getText(), Options.create()));
    }

    @Override
    public ProceduralPlan visitDropSchemaStatement(RelationalParser.DropSchemaStatementContext ctx) {
        final Pair<Optional<URI>, String> dbAndSchema = ParserUtils.parseSchemaIdentifier(ctx.uid().getText());
        Assert.thatUnchecked(dbAndSchema.getLeft().isPresent(), String.format("invalid database identifier in '%s'", ctx.uid().getText()));
        return ProceduralPlan.of(constantActionFactory.getDropSchemaConstantAction(dbAndSchema.getLeft().get(), dbAndSchema.getRight(), Options.create()));
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
        return filteredRecords;
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
