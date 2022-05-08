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
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.NotValue;
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
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
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;

import java.math.BigInteger;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Visits the abstract syntax tree of the query and generates a {@link com.apple.foundationdb.record.query.plan.cascades.RelationalExpression}
 * if the query is valid and supported.
 */
public class AstVisitor extends RelationalParserBaseVisitor<Typed> {

    @Nonnull
    private final ParserContext parserContext;

    @Nonnull
    private final GraphExpansion.Builder graphExpansionBuilder;

    // this field holds all the relations (record types) that we want to filter-scan from. Currently, it is needed
    // upfront by the planner, and it should become unnecessary later on.
    @Nonnull
    private final Set<String> filteredRecords;

    @Nullable
    private byte[] continuation;

    public static final String UNSUPPORTED_QUERY = "query is not supported";

    /**
     * Creates a new instance of {@link AstVisitor}.
     *
     * @param parserContext The parsing context.
     */
    public AstVisitor(@Nonnull final ParserContext parserContext) {
        this.parserContext = parserContext;
        this.graphExpansionBuilder = GraphExpansion.builder();
        this.filteredRecords = new LinkedHashSet<>();
    }

    @Override
    protected Typed aggregateResult(Typed aggregate, Typed nextResult) {
        return nextResult != null ? nextResult : aggregate;
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
    public Typed visitWithContinuationStatement(RelationalParser.WithContinuationStatementContext ctx) {
        // validate it is a valid continuation
        // sets member variable continuation
        String continuationStr = LiteralValue.ofScalar(ParserUtils.safeCast(visit(ctx.stringLiteral()), String.class)).getLiteralValue();
        Assert.notNullUnchecked(continuationStr, "continuation can not be null");
        continuation = Base64.getDecoder().decode(continuationStr);
        return null;
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
        Assert.thatUnchecked(ctx.indexHint().isEmpty(), UNSUPPORTED_QUERY);
        final String recordTypeName = ParserUtils.safeCast(visit(ctx.tableName()), String.class);
        Assert.notNullUnchecked(recordTypeName);
        final ImmutableSet<String> recordTypeNameSet = ImmutableSet.<String>builder().add(recordTypeName).build();
        final RecordMetaData recordMetaData = parserContext.getRecordMetaData();
        final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap = recordMetaData.getFieldDescriptorMapFromNames(recordTypeNameSet);
        final Set<String> allAvailableRecordTypes = recordMetaData.getRecordTypes().keySet();
        RelationalExpression typeFilterExpression = new LogicalTypeFilterExpression(recordTypeNameSet,
                Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(allAvailableRecordTypes))),
                Type.Record.fromFieldDescriptorsMap(fieldDescriptorMap));

        // improve
        final Quantifier.ForEach forEachQuantifier = Quantifier.forEachBuilder().build(GroupExpressionRef.of(typeFilterExpression));
        final QuantifiedObjectValue quantifiedObjectValue = forEachQuantifier.getFlowedObjectValue();
        graphExpansionBuilder.addQuantifier(forEachQuantifier);
        final String alias = (ctx.AS() != null) ? ParserUtils.safeCast(visit(ctx.alias), String.class) : recordTypeName;
        Assert.notNullUnchecked(alias);
        parserContext.pushScope(Map.of(CorrelationIdentifier.of(alias), quantifiedObjectValue));
        filteredRecords.addAll(recordTypeNameSet);
        return null;
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
    public Value visitIntervalExpr(RelationalParser.IntervalExprContext ctx) {
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
        result.append(ParserUtils.safeCast(visit(ctx.uid(0)), String.class));
        if (ctx.DOT_ID() != null) {
            result.append(ParserUtils.unquoteString(ctx.DOT_ID().getText()));
        } else {
            Assert.thatUnchecked(!ctx.uid().isEmpty());
            ctx.uid().stream().skip(1).forEach(d -> result.append(ParserUtils.safeCast(visit(d), String.class)));
        }
        return LiteralValue.ofScalar(result.toString());
    }

    @Override
    public Typed visitTableName(RelationalParser.TableNameContext ctx) {
        return (visit(ctx.fullId()));
    }

    @Override
    public Value visitFullColumnName(RelationalParser.FullColumnNameContext ctx) {
        if (!ctx.dottedId().isEmpty()) {
            Assert.thatUnchecked(ctx.dottedId().size() == 1, UNSUPPORTED_QUERY); // todo nesting
            final String recordTypeName = ParserUtils.safeCast(visit(ctx.uid()), String.class);
            Assert.notNullUnchecked(recordTypeName);
            final String fieldName = ParserUtils.safeCast(visit(ctx.dottedId(0)), String.class);
            Assert.notNullUnchecked(fieldName);
            Assert.thatUnchecked(fieldName.length() > 1);
            final FieldValue fieldValue = ParserUtils.getFieldValue(fieldName.substring(1), parserContext.resolveIdentifier(recordTypeName));
            Assert.notNullUnchecked(fieldValue, String.format("attempting to query non existing field %s%s", recordTypeName, fieldName));
            return fieldValue;
        } else {
            final String fieldName = ParserUtils.safeCast(visit(ctx.uid()), String.class);
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
            return LiteralValue.ofScalar(ParserUtils.safeCast(visit(ctx.simpleId()), String.class));
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
        return LiteralValue.ofScalar("." + ParserUtils.safeCast(visit(ctx.uid()), String.class));
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

    @Nonnull
    public Set<String> getFilteredRecords() {
        return filteredRecords;
    }

    @Nullable
    public byte[] getContinuation() {
        return continuation == null ? null : continuation.clone();
    }
}
