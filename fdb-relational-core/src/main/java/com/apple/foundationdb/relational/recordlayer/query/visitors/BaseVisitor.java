/*
 * BaseVisitor.java
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

package com.apple.foundationdb.relational.recordlayer.query.visitors;

import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperators;
import com.apple.foundationdb.relational.recordlayer.query.LogicalPlanFragment;
import com.apple.foundationdb.relational.recordlayer.query.MutablePlanGenerationContext;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.StringTrieNode;
import com.apple.foundationdb.relational.recordlayer.query.functions.FunctionCatalog;
import com.apple.foundationdb.relational.recordlayer.query.functions.SqlFunctionCatalog;
import com.apple.foundationdb.relational.util.Assert;

import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This class is a composition of different, specialized AST visitors. It holds that visitation and some other
 * cross-functional state.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class BaseVisitor extends AbstractParseTreeVisitor<Object> implements TypedVisitor {

    private final boolean caseSensitive;

    @Nonnull
    protected final MutablePlanGenerationContext mutablePlanGenerationContext;

    @Nonnull
    private final DdlQueryFactory ddlQueryFactory;

    @Nonnull
    private final MetadataOperationsFactory metadataOperationsFactory;

    @Nonnull
    private final URI dbUri;

    @Nonnull
    protected Optional<LogicalPlanFragment> currentPlanFragment;

    @Nonnull
    private final ExpressionVisitor expressionVisitor;

    @Nonnull
    private final IdentifierVisitor identifierVisitor;

    @Nonnull
    private final QueryVisitor queryVisitor;

    @Nonnull
    private final MetadataPlanVisitor metadataPlanVisitor;

    @Nonnull
    private final DdlVisitor ddlVisitor;

    @Nonnull
    private RecordLayerSchemaTemplate metadata;

    @Nonnull
    private SemanticAnalyzer semanticAnalyzer;

    public BaseVisitor(@Nonnull MutablePlanGenerationContext mutablePlanGenerationContext,
                       @Nonnull RecordLayerSchemaTemplate metadata,
                       @Nonnull DdlQueryFactory ddlQueryFactory,
                       @Nonnull MetadataOperationsFactory metadataOperationsFactory,
                       @Nonnull URI dbUri,
                       boolean caseSensitive) {
        this.mutablePlanGenerationContext = mutablePlanGenerationContext;
        this.metadata = metadata;
        this.ddlQueryFactory = ddlQueryFactory;
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.dbUri = dbUri;
        this.currentPlanFragment = Optional.empty();
        this.caseSensitive = caseSensitive;
        this.expressionVisitor = ExpressionVisitor.of(this);
        this.identifierVisitor = IdentifierVisitor.of(this);
        this.queryVisitor = QueryVisitor.of(this);
        this.metadataPlanVisitor = MetadataPlanVisitor.of(this);
        this.ddlVisitor = DdlVisitor.of(this, metadataOperationsFactory, dbUri);
        this.semanticAnalyzer = new SemanticAnalyzer(getCatalog(), getFunctionCatalog());
    }

    @Nonnull
    public MutablePlanGenerationContext getPlanGenerationContext() {
        return mutablePlanGenerationContext;
    }

    @Nonnull
    public Plan<?> generateLogicalPlan(@Nonnull ParseTree parseTree) {
        final var result = visit(parseTree);
        return Assert.castUnchecked(result, Plan.class, ErrorCode.INTERNAL_ERROR, () -> "Could not generate a logical plan");
    }

    @Nonnull
    public RecordLayerSchemaTemplate getCatalog() {
        return metadata;
    }

    @Nonnull
    public RecordLayerSchemaTemplate replaceCatalog(@Nonnull RecordLayerSchemaTemplate newCatalog) {
        final var oldMetadata = metadata;
        metadata = newCatalog;
        semanticAnalyzer = new SemanticAnalyzer(metadata, getFunctionCatalog());
        return oldMetadata;
    }

    @Nonnull
    public SemanticAnalyzer getSemanticAnalyzer() {
        return semanticAnalyzer;
    }

    @Nonnull
    public FunctionCatalog getFunctionCatalog() {
        return SqlFunctionCatalog.instance();
    }

    @Nonnull
    LogicalOperators getLogicalOperators() {
        return currentPlanFragment.orElseThrow().getLogicalOperators();
    }

    @Nonnull
    LogicalOperators getLogicalOperatorsIncludingOuter() {
        return currentPlanFragment.orElseThrow().getLogicalOperatorsIncludingOuter();
    }

    boolean isTopLevel() {
        return !(currentPlanFragment.isPresent() && currentPlanFragment.get().hasParent());
    }

    @Nonnull
    LogicalPlanFragment pushPlanFragment() {
        currentPlanFragment = Optional.of(currentPlanFragment.map(LogicalPlanFragment::addChild).orElse(LogicalPlanFragment.ofRoot()));
        return currentPlanFragment.get();
    }

    void popPlanFragment() {
        this.currentPlanFragment = currentPlanFragment.flatMap(LogicalPlanFragment::getParentMaybe);
    }

    @Nonnull
    LogicalPlanFragment getCurrentPlanFragment() {
        Assert.thatUnchecked(currentPlanFragment.isPresent());
        return currentPlanFragment.get();
    }

    @Nonnull
    Optional<LogicalPlanFragment> getCurrentPlanFragmentMaybe() {
        return currentPlanFragment;
    }

    boolean isForDdl() {
        return getPlanGenerationContext().isForDdl();
    }

    @Nonnull
    protected String normalizeString(@Nonnull final String value) {
        return Assert.notNullUnchecked(SemanticAnalyzer.normalizeString(value, caseSensitive));
    }

    @Nonnull
    public Expression resolveFunction(@Nonnull String functionName, @Nonnull Expression... arguments) {
        return getSemanticAnalyzer().resolveFunction(functionName, true, arguments);
    }

    @Nonnull
    public Expression resolveFunction(@Nonnull String functionName, boolean flattenSingleItemRecords, @Nonnull Expression... arguments) {
        return getSemanticAnalyzer().resolveFunction(functionName, flattenSingleItemRecords, arguments);
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        return nextResult != null ? nextResult : aggregate;
    }

    protected void setLimit(int limit) {
        // TODO: this is temporary until the context are completely removed when the old plan generator is removed.
        getPlanGenerationContext().setLimit(limit);
    }

    @Override
    @Nonnull
    public Object visitRoot(@Nonnull RelationalParser.RootContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSqlStatements(@Nonnull RelationalParser.SqlStatementsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSqlStatement(@Nonnull RelationalParser.SqlStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitDdlStatement(@Nonnull RelationalParser.DdlStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.LogicalQueryPlan visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx) {
        return queryVisitor.visitDmlStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitTransactionStatement(@Nonnull RelationalParser.TransactionStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitPreparedStatement(@Nonnull RelationalParser.PreparedStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitAdministrationStatement(@Nonnull RelationalParser.AdministrationStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitUtilityStatement(@Nonnull RelationalParser.UtilityStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitTemplateClause(@Nonnull RelationalParser.TemplateClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx) {
        return ddlVisitor.visitCreateSchemaStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        return ddlVisitor.visitCreateSchemaTemplateStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx) {
        return ddlVisitor.visitCreateDatabaseStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitOptionsClause(@Nonnull RelationalParser.OptionsClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitOption(@Nonnull RelationalParser.OptionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx) {
        return ddlVisitor.visitDropDatabaseStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return ddlVisitor.visitDropSchemaTemplateStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx) {
        return ddlVisitor.visitDropSchemaStatement(ctx);
    }

    @Override
    @Nonnull
    public RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx) {
        return ddlVisitor.visitStructDefinition(ctx);
    }

    @Override
    @Nonnull
    public RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx) {
        return ddlVisitor.visitTableDefinition(ctx);
    }

    @Override
    @Nonnull
    public Object visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx) {
        return ddlVisitor.visitColumnType(ctx);
    }

    @Override
    @Nonnull
    public DataType visitPrimitiveType(@Nonnull RelationalParser.PrimitiveTypeContext ctx) {
        return ddlVisitor.visitPrimitiveType(ctx);
    }

    @Override
    @Nonnull
    public Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx) {
        return ddlVisitor.visitNullColumnConstraint(ctx);
    }

    @Override
    @Nonnull
    public Object visitPrimaryKeyDefinition(@Nonnull RelationalParser.PrimaryKeyDefinitionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx) {
        return ddlVisitor.visitEnumDefinition(ctx);
    }

    @Override
    @Nonnull
    public RecordLayerIndex visitIndexDefinition(@Nonnull RelationalParser.IndexDefinitionContext ctx) {
        return ddlVisitor.visitIndexDefinition(ctx);
    }

    @Override
    public Object visitIndexAttributes(RelationalParser.IndexAttributesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitIndexAttribute(RelationalParser.IndexAttributeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCharSet(@Nonnull RelationalParser.CharSetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitIntervalType(@Nonnull RelationalParser.IntervalTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSchemaId(@Nonnull RelationalParser.SchemaIdContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitPath(@Nonnull RelationalParser.PathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSchemaTemplateId(@Nonnull RelationalParser.SchemaTemplateIdContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitDeleteStatement(@Nonnull RelationalParser.DeleteStatementContext ctx) {
        return queryVisitor.visitDeleteStatement(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitInsertStatement(@Nonnull RelationalParser.InsertStatementContext ctx) {
        return queryVisitor.visitInsertStatement(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitSelectStatementWithContinuation(@Nonnull RelationalParser.SelectStatementWithContinuationContext ctx) {
        return queryVisitor.visitSelectStatementWithContinuation(ctx);
    }

    @Override
    @Nonnull
    public Expression visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx) {
        return expressionVisitor.visitContinuationAtom(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitSimpleSelect(@Nonnull RelationalParser.SimpleSelectContext ctx) {
        return queryVisitor.visitSimpleSelect(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitParenthesisSelect(@Nonnull RelationalParser.ParenthesisSelectContext ctx) {
        return visitQueryExpression(ctx.queryExpression());
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionSimpleSelect(RelationalParser.UnionSimpleSelectContext ctx) {
        return visitUnionSelectSpecification(ctx.unionSelectSpecification());
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionParenthesisSelect(@Nonnull RelationalParser.UnionParenthesisSelectContext ctx) {
        return visitUnionSelectExpression(ctx.unionSelectExpression());
    }

    @Override
    @Nonnull
    public LogicalOperator visitParenthesisUnionSimpleSelect(RelationalParser.ParenthesisUnionSimpleSelectContext ctx) {
        return visitParenthesisUnionSelectSpecification(ctx.parenthesisUnionSelectSpecification());
    }

    @Override
    @Nonnull
    public LogicalOperator visitParenthesisUnionParenthesisSelect(RelationalParser.ParenthesisUnionParenthesisSelectContext ctx) {
        return visitParenthesisUnionSelectExpression(ctx.parenthesisUnionSelectExpression());
    }

    @Override
    @Nonnull
    public LogicalOperator visitInsertStatementValueSelect(@Nonnull RelationalParser.InsertStatementValueSelectContext ctx) {
        return queryVisitor.visitInsertStatementValueSelect(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitInsertStatementValueValues(@Nonnull RelationalParser.InsertStatementValueValuesContext ctx) {
        return queryVisitor.visitInsertStatementValueValues(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitUpdatedElement(@Nonnull RelationalParser.UpdatedElementContext ctx) {
        return expressionVisitor.visitUpdatedElement(ctx);
    }

    @Override
    @Nonnull
    public Object visitAssignmentField(@Nonnull RelationalParser.AssignmentFieldContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUpdateStatement(@Nonnull RelationalParser.UpdateStatementContext ctx) {
        return queryVisitor.visitUpdateStatement(ctx);
    }

    @Override
    @Nonnull
    public Pair<Boolean, Expressions> visitOrderByClause(@Nonnull RelationalParser.OrderByClauseContext ctx) {
        return expressionVisitor.visitOrderByClause(ctx);
    }

    @Override
    @Nonnull
    public Pair<Expression, Boolean> visitOrderByExpression(@Nonnull RelationalParser.OrderByExpressionContext ctx) {
        return expressionVisitor.visitOrderByExpression(ctx);
    }

    @Override
    @Nullable
    public Void visitTableSources(@Nonnull RelationalParser.TableSourcesContext ctx) {
        return queryVisitor.visitTableSources(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitTableSourceBase(@Nonnull RelationalParser.TableSourceBaseContext ctx) {
        return queryVisitor.visitTableSourceBase(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitAtomTableItem(@Nonnull RelationalParser.AtomTableItemContext ctx) {
        return queryVisitor.visitAtomTableItem(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitSubqueryTableItem(@Nonnull RelationalParser.SubqueryTableItemContext ctx) {
        return queryVisitor.visitSubqueryTableItem(ctx);
    }

    @Override
    @Nonnull
    public Set<String> visitIndexHint(@Nonnull RelationalParser.IndexHintContext ctx) {
        return queryVisitor.visitIndexHint(ctx);
    }

    @Override
    @Nonnull
    public Object visitIndexHintType(@Nonnull RelationalParser.IndexHintTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitInnerJoin(@Nonnull RelationalParser.InnerJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitStraightJoin(@Nonnull RelationalParser.StraightJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitOuterJoin(@Nonnull RelationalParser.OuterJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitNaturalJoin(@Nonnull RelationalParser.NaturalJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitQueryExpression(@Nonnull RelationalParser.QueryExpressionContext ctx) {
        return queryVisitor.visitQueryExpression(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitQuerySpecification(@Nonnull RelationalParser.QuerySpecificationContext ctx) {
        return queryVisitor.visitQuerySpecification(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionStatement(@Nonnull RelationalParser.UnionStatementContext ctx) {
        return queryVisitor.visitUnionStatement(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionSelectSpecification(RelationalParser.UnionSelectSpecificationContext ctx) {
        return queryVisitor.visitUnionSelectSpecification(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionSelectExpression(RelationalParser.UnionSelectExpressionContext ctx) {
        return queryVisitor.visitUnionSelectExpression(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitParenthesisUnionSelectSpecification(RelationalParser.ParenthesisUnionSelectSpecificationContext ctx) {
        return visitUnionSelectSpecification(ctx.unionSelectSpecification());
    }

    @Override
    @Nonnull
    public LogicalOperator visitParenthesisUnionSelectExpression(RelationalParser.ParenthesisUnionSelectExpressionContext ctx) {
        return visitUnionSelectExpression(ctx.unionSelectExpression());
    }

    @Override
    @Nonnull
    public Expressions visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx) {
        return expressionVisitor.visitSelectElements(ctx);
    }

    @Override
    @Nonnull
    public Expression visitSelectStarElement(@Nonnull RelationalParser.SelectStarElementContext ctx) {
        return expressionVisitor.visitSelectStarElement(ctx);
    }

    @Override
    @Nonnull
    public Object visitSelectQualifierStarElement(@Nonnull RelationalParser.SelectQualifierStarElementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitSelectExpressionElement(@Nonnull RelationalParser.SelectExpressionElementContext ctx) {
        return expressionVisitor.visitSelectExpressionElement(ctx);
    }

    @Override
    @Nullable
    public Void visitFromClause(@Nonnull RelationalParser.FromClauseContext ctx) {
        return queryVisitor.visitFromClause(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitGroupByClause(@Nonnull RelationalParser.GroupByClauseContext ctx) {
        return expressionVisitor.visitGroupByClause(ctx);
    }

    @Override
    @Nonnull
    public Expression visitWhereExpr(@Nonnull RelationalParser.WhereExprContext ctx) {
        return expressionVisitor.visitWhereExpr(ctx);
    }

    @Override
    @Nonnull
    public Expression visitHavingClause(@Nonnull RelationalParser.HavingClauseContext ctx) {
        return expressionVisitor.visitHavingClause(ctx);
    }

    @Override
    @Nonnull
    public Expression visitGroupByItem(@Nonnull RelationalParser.GroupByItemContext ctx) {
        return expressionVisitor.visitGroupByItem(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx) {
        return expressionVisitor.visitLimitClause(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLimitClauseAtom(@Nonnull RelationalParser.LimitClauseAtomContext ctx) {
        return expressionVisitor.visitLimitClauseAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitQueryOptions(@Nonnull RelationalParser.QueryOptionsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitQueryOption(@Nonnull RelationalParser.QueryOptionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitStartTransaction(@Nonnull RelationalParser.StartTransactionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCommitStatement(@Nonnull RelationalParser.CommitStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitRollbackStatement(@Nonnull RelationalParser.RollbackStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetAutocommitStatement(@Nonnull RelationalParser.SetAutocommitStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetTransactionStatement(@Nonnull RelationalParser.SetTransactionStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitTransactionOption(@Nonnull RelationalParser.TransactionOptionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitTransactionLevel(@Nonnull RelationalParser.TransactionLevelContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitPrepareStatement(@Nonnull RelationalParser.PrepareStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitExecuteStatement(@Nonnull RelationalParser.ExecuteStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(@Nonnull RelationalParser.ShowDatabasesStatementContext ctx) {
        return metadataPlanVisitor.visitShowDatabasesStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(@Nonnull RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        return metadataPlanVisitor.visitShowSchemaTemplatesStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetVariable(@Nonnull RelationalParser.SetVariableContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetCharset(@Nonnull RelationalParser.SetCharsetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetNames(@Nonnull RelationalParser.SetNamesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetTransaction(@Nonnull RelationalParser.SetTransactionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetAutocommit(@Nonnull RelationalParser.SetAutocommitContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetNewValueInsideTrigger(@Nonnull RelationalParser.SetNewValueInsideTriggerContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitVariableClause(@Nonnull RelationalParser.VariableClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitKillStatement(@Nonnull RelationalParser.KillStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitResetStatement(@Nonnull RelationalParser.ResetStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitTableIndexes(@Nonnull RelationalParser.TableIndexesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLoadedTableIndexes(@Nonnull RelationalParser.LoadedTableIndexesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(@Nonnull RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        return metadataPlanVisitor.visitSimpleDescribeSchemaStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(@Nonnull RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        return metadataPlanVisitor.visitSimpleDescribeSchemaTemplateStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.LogicalQueryPlan visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx) {
        return queryVisitor.visitFullDescribeStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitHelpStatement(@Nonnull RelationalParser.HelpStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitDescribeStatements(@Nonnull RelationalParser.DescribeStatementsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitDescribeConnection(@Nonnull RelationalParser.DescribeConnectionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitFullId(@Nonnull RelationalParser.FullIdContext ctx) {
        return identifierVisitor.visitFullId(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitTableName(@Nonnull RelationalParser.TableNameContext ctx) {
        return identifierVisitor.visitTableName(ctx);
    }

    @Override
    @Nonnull
    public Expression visitFullColumnName(@Nonnull RelationalParser.FullColumnNameContext ctx) {
        return expressionVisitor.visitFullColumnName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitIndexColumnName(@Nonnull RelationalParser.IndexColumnNameContext ctx) {
        return identifierVisitor.visitIndexColumnName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitCharsetName(@Nonnull RelationalParser.CharsetNameContext ctx) {
        return identifierVisitor.visitCharsetName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitCollationName(@Nonnull RelationalParser.CollationNameContext ctx) {
        return identifierVisitor.visitCollationName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitUid(@Nonnull RelationalParser.UidContext ctx) {
        return identifierVisitor.visitUid(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitSimpleId(@Nonnull RelationalParser.SimpleIdContext ctx) {
        return identifierVisitor.visitSimpleId(ctx);
    }

    @Override
    @Nonnull
    public Object visitNullNotnull(@Nonnull RelationalParser.NullNotnullContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitDecimalLiteral(@Nonnull RelationalParser.DecimalLiteralContext ctx) {
        return expressionVisitor.visitDecimalLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitStringLiteral(@Nonnull RelationalParser.StringLiteralContext ctx) {
        return expressionVisitor.visitStringLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBooleanLiteral(@Nonnull RelationalParser.BooleanLiteralContext ctx) {
        return expressionVisitor.visitBooleanLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBytesLiteral(@Nonnull RelationalParser.BytesLiteralContext ctx) {
        return expressionVisitor.visitBytesLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNullLiteral(@Nonnull RelationalParser.NullLiteralContext ctx) {
        return expressionVisitor.visitNullLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitStringConstant(@Nonnull RelationalParser.StringConstantContext ctx) {
        return expressionVisitor.visitStringConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitDecimalConstant(@Nonnull RelationalParser.DecimalConstantContext ctx) {
        return expressionVisitor.visitDecimalConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNegativeDecimalConstant(@Nonnull RelationalParser.NegativeDecimalConstantContext ctx) {
        return expressionVisitor.visitNegativeDecimalConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBytesConstant(@Nonnull RelationalParser.BytesConstantContext ctx) {
        return expressionVisitor.visitBytesConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBooleanConstant(@Nonnull RelationalParser.BooleanConstantContext ctx) {
        return expressionVisitor.visitBooleanConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBitStringConstant(@Nonnull RelationalParser.BitStringConstantContext ctx) {
        return expressionVisitor.visitBitStringConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNullConstant(@Nonnull RelationalParser.NullConstantContext ctx) {
        return expressionVisitor.visitNullConstant(ctx);
    }

    @Override
    @Nonnull
    public Object visitStringDataType(@Nonnull RelationalParser.StringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitNationalStringDataType(@Nonnull RelationalParser.NationalStringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitNationalVaryingStringDataType(@Nonnull RelationalParser.NationalVaryingStringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitDimensionDataType(@Nonnull RelationalParser.DimensionDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSimpleDataType(@Nonnull RelationalParser.SimpleDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCollectionDataType(@Nonnull RelationalParser.CollectionDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSpatialDataType(@Nonnull RelationalParser.SpatialDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLongVarcharDataType(@Nonnull RelationalParser.LongVarcharDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLongVarbinaryDataType(@Nonnull RelationalParser.LongVarbinaryDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCollectionOptions(@Nonnull RelationalParser.CollectionOptionsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitConvertedDataType(@Nonnull RelationalParser.ConvertedDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLengthOneDimension(@Nonnull RelationalParser.LengthOneDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLengthTwoDimension(@Nonnull RelationalParser.LengthTwoDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLengthTwoOptionalDimension(@Nonnull RelationalParser.LengthTwoOptionalDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public List<Identifier> visitUidList(@Nonnull RelationalParser.UidListContext ctx) {
        return List.of();
    }

    @Override
    @Nonnull
    public Pair<String, StringTrieNode> visitUidWithNestings(@Nonnull RelationalParser.UidWithNestingsContext ctx) {
        return expressionVisitor.visitUidWithNestings(ctx);
    }

    @Override
    @Nonnull
    public StringTrieNode visitUidListWithNestingsInParens(@Nonnull RelationalParser.UidListWithNestingsInParensContext ctx) {
        return expressionVisitor.visitUidListWithNestingsInParens(ctx);
    }

    @Override
    @Nonnull
    public StringTrieNode visitUidListWithNestings(@Nonnull RelationalParser.UidListWithNestingsContext ctx) {
        return expressionVisitor.visitUidListWithNestings(ctx);
    }

    @Override
    @Nonnull
    public Object visitTables(@Nonnull RelationalParser.TablesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitIndexColumnNames(@Nonnull RelationalParser.IndexColumnNamesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitExpressions(@Nonnull RelationalParser.ExpressionsContext ctx) {
        return expressionVisitor.visitExpressions(ctx);
    }

    @Override
    @Nonnull
    public Object visitExpressionsWithDefaults(@Nonnull RelationalParser.ExpressionsWithDefaultsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitRecordConstructorForInsert(@Nonnull RelationalParser.RecordConstructorForInsertContext ctx) {
        return expressionVisitor.visitRecordConstructorForInsert(ctx);
    }

    @Override
    @Nonnull
    public Expression visitRecordConstructor(@Nonnull RelationalParser.RecordConstructorContext ctx) {
        return expressionVisitor.visitRecordConstructor(ctx);
    }

    @Override
    @Nonnull
    public Object visitOfTypeClause(@Nonnull RelationalParser.OfTypeClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitArrayConstructor(@Nonnull RelationalParser.ArrayConstructorContext ctx) {
        return expressionVisitor.visitArrayConstructor(ctx);
    }

    @Override
    @Nonnull
    public Object visitUserVariables(@Nonnull RelationalParser.UserVariablesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitDefaultValue(@Nonnull RelationalParser.DefaultValueContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCurrentTimestamp(@Nonnull RelationalParser.CurrentTimestampContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitExpressionOrDefault(@Nonnull RelationalParser.ExpressionOrDefaultContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitExpressionWithName(@Nonnull RelationalParser.ExpressionWithNameContext ctx) {
        return expressionVisitor.visitExpressionWithName(ctx);
    }

    @Override
    @Nonnull
    public Expression visitExpressionWithOptionalName(@Nonnull RelationalParser.ExpressionWithOptionalNameContext ctx) {
        return expressionVisitor.visitExpressionWithOptionalName(ctx);
    }

    @Override
    @Nonnull
    public Object visitIfExists(@Nonnull RelationalParser.IfExistsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitIfNotExists(@Nonnull RelationalParser.IfNotExistsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitAggregateFunctionCall(@Nonnull RelationalParser.AggregateFunctionCallContext ctx) {
        return expressionVisitor.visitAggregateFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitSpecificFunctionCall(@Nonnull RelationalParser.SpecificFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx) {
        return expressionVisitor.visitScalarFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitSimpleFunctionCall(@Nonnull RelationalParser.SimpleFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitDataTypeFunctionCall(@Nonnull RelationalParser.DataTypeFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitValuesFunctionCall(@Nonnull RelationalParser.ValuesFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCaseExpressionFunctionCall(@Nonnull RelationalParser.CaseExpressionFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitCaseFunctionCall(@Nonnull RelationalParser.CaseFunctionCallContext ctx) {
        return expressionVisitor.visitCaseFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitCharFunctionCall(@Nonnull RelationalParser.CharFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitPositionFunctionCall(@Nonnull RelationalParser.PositionFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitSubstrFunctionCall(@Nonnull RelationalParser.SubstrFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitTrimFunctionCall(@Nonnull RelationalParser.TrimFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitWeightFunctionCall(@Nonnull RelationalParser.WeightFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitExtractFunctionCall(@Nonnull RelationalParser.ExtractFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitGetFormatFunctionCall(@Nonnull RelationalParser.GetFormatFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCaseFuncAlternative(@Nonnull RelationalParser.CaseFuncAlternativeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLevelWeightList(@Nonnull RelationalParser.LevelWeightListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLevelWeightRange(@Nonnull RelationalParser.LevelWeightRangeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLevelInWeightListElement(@Nonnull RelationalParser.LevelInWeightListElementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitAggregateWindowedFunction(@Nonnull RelationalParser.AggregateWindowedFunctionContext ctx) {
        return expressionVisitor.visitAggregateWindowedFunction(ctx);
    }

    @Override
    @Nonnull
    public Object visitNonAggregateWindowedFunction(@Nonnull RelationalParser.NonAggregateWindowedFunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitOverClause(@Nonnull RelationalParser.OverClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitWindowName(@Nonnull RelationalParser.WindowNameContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitScalarFunctionName(@Nonnull RelationalParser.ScalarFunctionNameContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitFunctionArgs(@Nonnull RelationalParser.FunctionArgsContext ctx) {
        return expressionVisitor.visitFunctionArgs(ctx);
    }

    @Override
    @Nonnull
    public Object visitFunctionArg(@Nonnull RelationalParser.FunctionArgContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitIsExpression(@Nonnull RelationalParser.IsExpressionContext ctx) {
        return expressionVisitor.visitIsExpression(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNotExpression(@Nonnull RelationalParser.NotExpressionContext ctx) {
        return expressionVisitor.visitNotExpression(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLikePredicate(@Nonnull RelationalParser.LikePredicateContext ctx) {
        return expressionVisitor.visitLikePredicate(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLogicalExpression(@Nonnull RelationalParser.LogicalExpressionContext ctx) {
        return expressionVisitor.visitLogicalExpression(ctx);
    }

    @Override
    @Nonnull
    public Object visitPredicateExpression(@Nonnull RelationalParser.PredicateExpressionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitExpressionAtomPredicate(@Nonnull RelationalParser.ExpressionAtomPredicateContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBinaryComparisonPredicate(@Nonnull RelationalParser.BinaryComparisonPredicateContext ctx) {
        return expressionVisitor.visitBinaryComparisonPredicate(ctx);
    }

    @Override
    @Nonnull
    public Expression visitInPredicate(@Nonnull RelationalParser.InPredicateContext ctx) {
        return expressionVisitor.visitInPredicate(ctx);
    }

    @Override
    @Nonnull
    public Expression visitInList(@Nonnull RelationalParser.InListContext ctx) {
        return expressionVisitor.visitInList(ctx);
    }

    @Override
    @Nonnull
    public Object visitJsonExpressionAtom(@Nonnull RelationalParser.JsonExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitSubqueryExpressionAtom(@Nonnull RelationalParser.SubqueryExpressionAtomContext ctx) {
        return expressionVisitor.visitSubqueryExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitConstantExpressionAtom(@Nonnull RelationalParser.ConstantExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitFunctionCallExpressionAtom(@Nonnull RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return expressionVisitor.visitFunctionCallExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitFullColumnNameExpressionAtom(@Nonnull RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBitExpressionAtom(@Nonnull RelationalParser.BitExpressionAtomContext ctx) {
        return expressionVisitor.visitBitExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitPreparedStatementParameterAtom(@Nonnull RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return expressionVisitor.visitPreparedStatementParameterAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitRecordConstructorExpressionAtom(@Nonnull RelationalParser.RecordConstructorExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitArrayConstructorExpressionAtom(@Nonnull RelationalParser.ArrayConstructorExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitMathExpressionAtom(@Nonnull RelationalParser.MathExpressionAtomContext ctx) {
        return expressionVisitor.visitMathExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitExistsExpressionAtom(@Nonnull RelationalParser.ExistsExpressionAtomContext ctx) {
        return expressionVisitor.visitExistsExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitIntervalExpressionAtom(@Nonnull RelationalParser.IntervalExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Expression visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx) {
        return expressionVisitor.visitPreparedStatementParameter(ctx);
    }

    @Override
    @Nonnull
    public Object visitUnaryOperator(@Nonnull RelationalParser.UnaryOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitComparisonOperator(@Nonnull RelationalParser.ComparisonOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitLogicalOperator(@Nonnull RelationalParser.LogicalOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitBitOperator(@Nonnull RelationalParser.BitOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitMathOperator(@Nonnull RelationalParser.MathOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitJsonOperator(@Nonnull RelationalParser.JsonOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitCharsetNameBase(@Nonnull RelationalParser.CharsetNameBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitIntervalTypeBase(@Nonnull RelationalParser.IntervalTypeBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitKeywordsCanBeId(@Nonnull RelationalParser.KeywordsCanBeIdContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitFunctionNameBase(@Nonnull RelationalParser.FunctionNameBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    @Nonnull
    public Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    public DdlQueryFactory getDdlQueryFactory() {
        return ddlQueryFactory;
    }

    @Nonnull
    public MetadataOperationsFactory getMetadataOperationsFactory() {
        return metadataOperationsFactory;
    }

    @Nonnull
    public URI getDbUri() {
        return dbUri;
    }
}
