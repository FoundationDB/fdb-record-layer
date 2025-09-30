/*
 * BaseVisitor.java
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserBaseVisitor;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperatorCatalog;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperators;
import com.apple.foundationdb.relational.recordlayer.query.LogicalPlanFragment;
import com.apple.foundationdb.relational.recordlayer.query.MutablePlanGenerationContext;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompilableSqlFunction;
import com.apple.foundationdb.relational.recordlayer.query.functions.SqlFunctionCatalog;
import com.apple.foundationdb.relational.util.Assert;
import org.antlr.v4.runtime.tree.ParseTree;

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
@API(API.Status.EXPERIMENTAL)
public class BaseVisitor extends RelationalParserBaseVisitor<Object> implements TypedVisitor {

    private final boolean caseSensitive;

    @Nonnull
    protected final MutablePlanGenerationContext mutablePlanGenerationContext;

    @Nonnull
    private final DdlQueryFactory ddlQueryFactory;

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

    @Nonnull
    private final LogicalOperatorCatalog logicalOperatorCatalog;

    public BaseVisitor(@Nonnull MutablePlanGenerationContext mutablePlanGenerationContext,
                       @Nonnull RecordLayerSchemaTemplate metadata,
                       @Nonnull DdlQueryFactory ddlQueryFactory,
                       @Nonnull MetadataOperationsFactory metadataOperationsFactory,
                       @Nonnull URI dbUri,
                       boolean caseSensitive) {
        this.mutablePlanGenerationContext = mutablePlanGenerationContext;
        this.metadata = metadata;
        this.ddlQueryFactory = ddlQueryFactory;
        this.dbUri = dbUri;
        this.currentPlanFragment = Optional.empty();
        this.caseSensitive = caseSensitive;
        this.expressionVisitor = ExpressionVisitor.of(this);
        this.identifierVisitor = IdentifierVisitor.of(this);
        this.queryVisitor = QueryVisitor.of(this);
        this.metadataPlanVisitor = MetadataPlanVisitor.of(this);
        this.ddlVisitor = DdlVisitor.of(this, metadataOperationsFactory, dbUri);
        this.semanticAnalyzer = new SemanticAnalyzer(getSchemaTemplate(), createFunctionCatalog(getSchemaTemplate()), mutablePlanGenerationContext);
        this.logicalOperatorCatalog = LogicalOperatorCatalog.newInstance();
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
    public RecordLayerSchemaTemplate getSchemaTemplate() {
        return metadata;
    }

    @Nonnull
    public RecordLayerSchemaTemplate replaceSchemaTemplate(@Nonnull RecordLayerSchemaTemplate newCatalog) {
        final var oldMetadata = metadata;
        metadata = newCatalog;
        semanticAnalyzer = new SemanticAnalyzer(metadata, createFunctionCatalog(metadata), mutablePlanGenerationContext);
        return oldMetadata;
    }

    @Nonnull
    public SemanticAnalyzer getSemanticAnalyzer() {
        return semanticAnalyzer;
    }

    @Nonnull
    public LogicalOperatorCatalog getLogicalOperatorCatalog() {
        return logicalOperatorCatalog;
    }

    @Nonnull
    public SqlFunctionCatalog createFunctionCatalog(@Nonnull final RecordLayerSchemaTemplate metadata) {
        return SqlFunctionCatalog.newInstance(metadata, isCaseSensitive());
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    @Nonnull
    LogicalOperators getLogicalOperators() {
        return currentPlanFragment.orElseThrow().getLogicalOperators();
    }

    @Nonnull
    LogicalOperators getLogicalOperatorsIncludingOuter() {
        return currentPlanFragment.orElseThrow().getLogicalOperatorsIncludingOuter();
    }

    public boolean isTopLevel() {
        return !(currentPlanFragment.isPresent() && currentPlanFragment.get().hasParent());
    }

    @Nonnull
    public LogicalPlanFragment pushPlanFragment() {
        currentPlanFragment = Optional.of(currentPlanFragment.map(LogicalPlanFragment::addChild).orElse(LogicalPlanFragment.ofRoot()));
        return currentPlanFragment.get();
    }

    public void popPlanFragment() {
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
        return getSemanticAnalyzer().resolveScalarFunction(functionName, Expressions.of(arguments), true);
    }

    @Nonnull
    public Expression resolveFunction(@Nonnull String functionName, boolean flattenSingleItemRecords, @Nonnull Expression... arguments) {
        return getSemanticAnalyzer().resolveScalarFunction(functionName, Expressions.of(arguments), flattenSingleItemRecords);
    }

    @Nonnull
    public LogicalOperator resolveTableValuedFunction(@Nonnull Identifier functionName, @Nonnull Expressions arguments) {
        return getSemanticAnalyzer().resolveTableFunction(functionName, arguments, true);
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        return nextResult != null ? nextResult : aggregate;
    }

    @Nonnull
    @Override
    public Object visitRoot(@Nonnull RelationalParser.RootContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitStatements(@Nonnull RelationalParser.StatementsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitStatement(@Nonnull RelationalParser.StatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx) {
        return queryVisitor.visitDmlStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitTransactionStatement(@Nonnull RelationalParser.TransactionStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitPreparedStatement(@Nonnull RelationalParser.PreparedStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitAdministrationStatement(@Nonnull RelationalParser.AdministrationStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitUtilityStatement(@Nonnull RelationalParser.UtilityStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitTemplateClause(@Nonnull RelationalParser.TemplateClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx) {
        return ddlVisitor.visitCreateSchemaStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        return ddlVisitor.visitCreateSchemaTemplateStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx) {
        return ddlVisitor.visitCreateDatabaseStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitOptionsClause(@Nonnull RelationalParser.OptionsClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitOption(@Nonnull RelationalParser.OptionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx) {
        return ddlVisitor.visitDropDatabaseStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return ddlVisitor.visitDropSchemaTemplateStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx) {
        return ddlVisitor.visitDropSchemaStatement(ctx);
    }

    @Nonnull
    @Override
    public RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx) {
        return ddlVisitor.visitStructDefinition(ctx);
    }

    @Nonnull
    @Override
    public RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx) {
        return ddlVisitor.visitTableDefinition(ctx);
    }

    @Nonnull
    @Override
    public Object visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public DataType visitFunctionColumnType(@Nonnull final RelationalParser.FunctionColumnTypeContext ctx) {
        return ddlVisitor.visitFunctionColumnType(ctx);
    }

    @Nonnull
    @Override
    public DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx) {
        return ddlVisitor.visitColumnType(ctx);
    }

    @Nonnull
    @Override
    public DataType visitPrimitiveType(@Nonnull RelationalParser.PrimitiveTypeContext ctx) {
        return ddlVisitor.visitPrimitiveType(ctx);
    }

    @Nonnull
    @Override
    public Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx) {
        return ddlVisitor.visitNullColumnConstraint(ctx);
    }

    @Nonnull
    @Override
    public Object visitPrimaryKeyDefinition(@Nonnull RelationalParser.PrimaryKeyDefinitionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public List<Identifier> visitFullIdList(RelationalParser.FullIdListContext ctx) {
        return identifierVisitor.visitFullIdList(ctx);
    }

    @Nonnull
    @Override
    public DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx) {
        return ddlVisitor.visitEnumDefinition(ctx);
    }

    @Nonnull
    @Override
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
    public ProceduralPlan visitCreateTempFunction(final RelationalParser.CreateTempFunctionContext ctx) {
        return ddlVisitor.visitCreateTempFunction(ctx);
    }

    @Override
    public ProceduralPlan visitDropTempFunction(final RelationalParser.DropTempFunctionContext ctx) {
        return ddlVisitor.visitDropTempFunction(ctx);
    }

    @Override
    public CompilableSqlFunction visitTempSqlInvokedFunction(final RelationalParser.TempSqlInvokedFunctionContext ctx) {
        return ddlVisitor.visitTempSqlInvokedFunction(ctx);
    }

    @Override
    public com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction visitSqlInvokedFunction(RelationalParser.SqlInvokedFunctionContext ctx) {
        return ddlVisitor.visitSqlInvokedFunction(ctx);
    }

    @Override
    public LogicalOperator visitStatementBody(final RelationalParser.StatementBodyContext ctx) {
        return ddlVisitor.visitStatementBody(ctx);
    }

    @Override
    public Identifier visitUserDefinedScalarFunctionStatementBody(@Nonnull RelationalParser.UserDefinedScalarFunctionStatementBodyContext ctx) {
        return identifierVisitor.visitFullId(ctx.fullId());
    }

    @Override
    public Object visitExpressionBody(final RelationalParser.ExpressionBodyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expressions visitSqlParameterDeclarationList(RelationalParser.SqlParameterDeclarationListContext ctx) {
        return ddlVisitor.visitSqlParameterDeclarationList(ctx);
    }

    @Override
    public Expressions visitSqlParameterDeclarations(final RelationalParser.SqlParameterDeclarationsContext ctx) {
        return ddlVisitor.visitSqlParameterDeclarations(ctx);
    }

    @Override
    public Expression visitSqlParameterDeclaration(RelationalParser.SqlParameterDeclarationContext ctx) {
        return ddlVisitor.visitSqlParameterDeclaration(ctx);
    }

    @Override
    public DataType visitReturnsType(RelationalParser.ReturnsTypeContext ctx) {
        return ddlVisitor.visitReturnsType(ctx);
    }

    @Override
    public RecordLayerInvokedRoutine visitFunctionSpecification(final RelationalParser.FunctionSpecificationContext ctx) {
        return ddlVisitor.visitFunctionSpecification(ctx);
    }

    @Override
    public Object visitParameterMode(final RelationalParser.ParameterModeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitReturnsClause(final RelationalParser.ReturnsClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitReturnsTableType(final RelationalParser.ReturnsTableTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTableFunctionColumnList(final RelationalParser.TableFunctionColumnListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTableFunctionColumnListElement(final RelationalParser.TableFunctionColumnListElementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitRoutineCharacteristics(final RelationalParser.RoutineCharacteristicsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLanguageClause(final RelationalParser.LanguageClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLanguageName(final RelationalParser.LanguageNameContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitParameterStyle(final RelationalParser.ParameterStyleContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitDeterministicCharacteristic(final RelationalParser.DeterministicCharacteristicContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitNullCallClause(final RelationalParser.NullCallClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitDispatchClause(final RelationalParser.DispatchClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public LogicalOperator visitSqlReturnStatement(final RelationalParser.SqlReturnStatementContext ctx) {
        return ddlVisitor.visitSqlReturnStatement(ctx);
    }

    @Override
    public LogicalOperator visitReturnValue(final RelationalParser.ReturnValueContext ctx) {
        return ddlVisitor.visitReturnValue(ctx);
    }

    @Nonnull
    @Override
    public Object visitCharSet(@Nonnull RelationalParser.CharSetContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitIntervalType(@Nonnull RelationalParser.IntervalTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSchemaId(@Nonnull RelationalParser.SchemaIdContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitPath(@Nonnull RelationalParser.PathContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSchemaTemplateId(@Nonnull RelationalParser.SchemaTemplateIdContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitDeleteStatement(@Nonnull RelationalParser.DeleteStatementContext ctx) {
        return queryVisitor.visitDeleteStatement(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatement(@Nonnull RelationalParser.InsertStatementContext ctx) {
        return queryVisitor.visitInsertStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitSelectStatement(RelationalParser.SelectStatementContext ctx) {
        return queryVisitor.visitSelectStatement(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitQuery(@Nonnull RelationalParser.QueryContext ctx) {
        return queryVisitor.visitQuery(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperators visitCtes(RelationalParser.CtesContext ctx) {
        return queryVisitor.visitCtes(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitNamedQuery(RelationalParser.NamedQueryContext ctx) {
        return queryVisitor.visitNamedQuery(ctx);
    }

    @Override
    public LogicalOperator visitTableFunction(@Nonnull final RelationalParser.TableFunctionContext ctx) {
        return expressionVisitor.visitTableFunction(ctx);
    }

    @Override
    public Expressions visitTableFunctionArgs(final RelationalParser.TableFunctionArgsContext ctx) {
        return expressionVisitor.visitTableFunctionArgs(ctx);
    }

    @Override
    public Identifier visitTableFunctionName(final RelationalParser.TableFunctionNameContext ctx) {
        return identifierVisitor.visitTableFunctionName(ctx);
    }

    @Nonnull
    @Override
    public Expression visitContinuation(RelationalParser.ContinuationContext ctx) {
        return expressionVisitor.visitContinuation(ctx);
    }

    @Nonnull
    @Override
    public Expression visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx) {
        return expressionVisitor.visitContinuationAtom(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitQueryTermDefault(@Nonnull RelationalParser.QueryTermDefaultContext ctx) {
        return Assert.castUnchecked(visitChildren(ctx), LogicalOperator.class);
    }

    @Nonnull
    @Override
    public LogicalOperator visitSetQuery(RelationalParser.SetQueryContext ctx) {
        return queryVisitor.visitSetQuery(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatementValueSelect(@Nonnull RelationalParser.InsertStatementValueSelectContext ctx) {
        return queryVisitor.visitInsertStatementValueSelect(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatementValueValues(@Nonnull RelationalParser.InsertStatementValueValuesContext ctx) {
        return queryVisitor.visitInsertStatementValueValues(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitUpdatedElement(@Nonnull RelationalParser.UpdatedElementContext ctx) {
        return expressionVisitor.visitUpdatedElement(ctx);
    }

    @Nonnull
    @Override
    public Object visitAssignmentField(@Nonnull RelationalParser.AssignmentFieldContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitUpdateStatement(@Nonnull RelationalParser.UpdateStatementContext ctx) {
        return queryVisitor.visitUpdateStatement(ctx);
    }

    @Nonnull
    @Override
    public List<OrderByExpression> visitOrderByClause(@Nonnull RelationalParser.OrderByClauseContext ctx) {
        return expressionVisitor.visitOrderByClause(ctx);
    }

    @Nonnull
    @Override
    public OrderByExpression visitOrderByExpression(@Nonnull RelationalParser.OrderByExpressionContext ctx) {
        return expressionVisitor.visitOrderByExpression(ctx);
    }

    @Override
    @Nullable
    public Void visitTableSources(@Nonnull RelationalParser.TableSourcesContext ctx) {
        return queryVisitor.visitTableSources(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitTableSourceBase(@Nonnull RelationalParser.TableSourceBaseContext ctx) {
        return queryVisitor.visitTableSourceBase(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitAtomTableItem(@Nonnull RelationalParser.AtomTableItemContext ctx) {
        return queryVisitor.visitAtomTableItem(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitSubqueryTableItem(@Nonnull RelationalParser.SubqueryTableItemContext ctx) {
        return queryVisitor.visitSubqueryTableItem(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInlineTableItem(@Nonnull RelationalParser.InlineTableItemContext ctx) {
        return queryVisitor.visitInlineTableItem(ctx);
    }

    @Override
    public LogicalOperator visitTableValuedFunction(@Nonnull final RelationalParser.TableValuedFunctionContext ctx) {
        return queryVisitor.visitTableValuedFunction(ctx);
    }

    @Nonnull
    @Override
    public Set<String> visitIndexHint(@Nonnull RelationalParser.IndexHintContext ctx) {
        return queryVisitor.visitIndexHint(ctx);
    }

    @Nonnull
    @Override
    public Object visitIndexHintType(@Nonnull RelationalParser.IndexHintTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> visitInlineTableDefinition(final RelationalParser.InlineTableDefinitionContext ctx) {
        return expressionVisitor.visitInlineTableDefinition(ctx);
    }

    @Nonnull
    @Override
    public Object visitInnerJoin(@Nonnull RelationalParser.InnerJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitStraightJoin(@Nonnull RelationalParser.StraightJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitOuterJoin(@Nonnull RelationalParser.OuterJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitNaturalJoin(@Nonnull RelationalParser.NaturalJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitSimpleTable(@Nonnull RelationalParser.SimpleTableContext ctx) {
        return queryVisitor.visitSimpleTable(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitParenthesisQuery(RelationalParser.ParenthesisQueryContext ctx) {
        return queryVisitor.visitParenthesisQuery(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx) {
        return expressionVisitor.visitSelectElements(ctx);
    }

    @Nonnull
    @Override
    public Expression visitSelectStarElement(@Nonnull RelationalParser.SelectStarElementContext ctx) {
        return expressionVisitor.visitSelectStarElement(ctx);
    }

    @Nonnull
    @Override
    public Object visitSelectQualifierStarElement(@Nonnull RelationalParser.SelectQualifierStarElementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitSelectExpressionElement(@Nonnull RelationalParser.SelectExpressionElementContext ctx) {
        return expressionVisitor.visitSelectExpressionElement(ctx);
    }

    @Override
    @Nullable
    public Void visitFromClause(@Nonnull RelationalParser.FromClauseContext ctx) {
        return queryVisitor.visitFromClause(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitGroupByClause(@Nonnull RelationalParser.GroupByClauseContext ctx) {
        return expressionVisitor.visitGroupByClause(ctx);
    }

    @Nonnull
    @Override
    public Expression visitWhereExpr(@Nonnull RelationalParser.WhereExprContext ctx) {
        return expressionVisitor.visitWhereExpr(ctx);
    }

    @Nonnull
    @Override
    public Expression visitHavingClause(@Nonnull RelationalParser.HavingClauseContext ctx) {
        return expressionVisitor.visitHavingClause(ctx);
    }

    @Nonnull
    @Override
    public Expression visitGroupByItem(@Nonnull RelationalParser.GroupByItemContext ctx) {
        return expressionVisitor.visitGroupByItem(ctx);
    }

    @Nonnull
    @Override
    public Expression visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx) {
        return expressionVisitor.visitLimitClause(ctx);
    }

    @Nonnull
    @Override
    public Expression visitLimitClauseAtom(@Nonnull RelationalParser.LimitClauseAtomContext ctx) {
        return expressionVisitor.visitLimitClauseAtom(ctx);
    }

    @Nonnull
    @Override
    public Object visitQueryOptions(@Nonnull RelationalParser.QueryOptionsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitQueryOption(@Nonnull RelationalParser.QueryOptionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitStartTransaction(@Nonnull RelationalParser.StartTransactionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitCommitStatement(@Nonnull RelationalParser.CommitStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitRollbackStatement(@Nonnull RelationalParser.RollbackStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetAutocommitStatement(@Nonnull RelationalParser.SetAutocommitStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetTransactionStatement(@Nonnull RelationalParser.SetTransactionStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitTransactionOption(@Nonnull RelationalParser.TransactionOptionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitTransactionLevel(@Nonnull RelationalParser.TransactionLevelContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitPrepareStatement(@Nonnull RelationalParser.PrepareStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitExecuteStatement(@Nonnull RelationalParser.ExecuteStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(@Nonnull RelationalParser.ShowDatabasesStatementContext ctx) {
        return metadataPlanVisitor.visitShowDatabasesStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(@Nonnull RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        return metadataPlanVisitor.visitShowSchemaTemplatesStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetVariable(@Nonnull RelationalParser.SetVariableContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetCharset(@Nonnull RelationalParser.SetCharsetContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetNames(@Nonnull RelationalParser.SetNamesContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetTransaction(@Nonnull RelationalParser.SetTransactionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetAutocommit(@Nonnull RelationalParser.SetAutocommitContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetNewValueInsideTrigger(@Nonnull RelationalParser.SetNewValueInsideTriggerContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitVariableClause(@Nonnull RelationalParser.VariableClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitKillStatement(@Nonnull RelationalParser.KillStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitResetStatement(@Nonnull RelationalParser.ResetStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitTableIndexes(@Nonnull RelationalParser.TableIndexesContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLoadedTableIndexes(@Nonnull RelationalParser.LoadedTableIndexesContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(@Nonnull RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        return metadataPlanVisitor.visitSimpleDescribeSchemaStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(@Nonnull RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        return metadataPlanVisitor.visitSimpleDescribeSchemaTemplateStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx) {
        return queryVisitor.visitFullDescribeStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitHelpStatement(@Nonnull RelationalParser.HelpStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitDescribeStatements(@Nonnull RelationalParser.DescribeStatementsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitDescribeConnection(@Nonnull RelationalParser.DescribeConnectionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitFullId(@Nonnull RelationalParser.FullIdContext ctx) {
        return identifierVisitor.visitFullId(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitTableName(@Nonnull RelationalParser.TableNameContext ctx) {
        return identifierVisitor.visitTableName(ctx);
    }

    @Nonnull
    @Override
    public Expression visitFullColumnName(@Nonnull RelationalParser.FullColumnNameContext ctx) {
        return expressionVisitor.visitFullColumnName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitIndexColumnName(@Nonnull RelationalParser.IndexColumnNameContext ctx) {
        return identifierVisitor.visitIndexColumnName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitCharsetName(@Nonnull RelationalParser.CharsetNameContext ctx) {
        return identifierVisitor.visitCharsetName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitCollationName(@Nonnull RelationalParser.CollationNameContext ctx) {
        return identifierVisitor.visitCollationName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitUid(@Nonnull RelationalParser.UidContext ctx) {
        return identifierVisitor.visitUid(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitSimpleId(@Nonnull RelationalParser.SimpleIdContext ctx) {
        return identifierVisitor.visitSimpleId(ctx);
    }

    @Nonnull
    @Override
    public Object visitNullNotnull(@Nonnull RelationalParser.NullNotnullContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitDecimalLiteral(@Nonnull RelationalParser.DecimalLiteralContext ctx) {
        return expressionVisitor.visitDecimalLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitStringLiteral(@Nonnull RelationalParser.StringLiteralContext ctx) {
        return expressionVisitor.visitStringLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBooleanLiteral(@Nonnull RelationalParser.BooleanLiteralContext ctx) {
        return expressionVisitor.visitBooleanLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBytesLiteral(@Nonnull RelationalParser.BytesLiteralContext ctx) {
        return expressionVisitor.visitBytesLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNullLiteral(@Nonnull RelationalParser.NullLiteralContext ctx) {
        return expressionVisitor.visitNullLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitStringConstant(@Nonnull RelationalParser.StringConstantContext ctx) {
        return expressionVisitor.visitStringConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitDecimalConstant(@Nonnull RelationalParser.DecimalConstantContext ctx) {
        return expressionVisitor.visitDecimalConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNegativeDecimalConstant(@Nonnull RelationalParser.NegativeDecimalConstantContext ctx) {
        return expressionVisitor.visitNegativeDecimalConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBytesConstant(@Nonnull RelationalParser.BytesConstantContext ctx) {
        return expressionVisitor.visitBytesConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBooleanConstant(@Nonnull RelationalParser.BooleanConstantContext ctx) {
        return expressionVisitor.visitBooleanConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBitStringConstant(@Nonnull RelationalParser.BitStringConstantContext ctx) {
        return expressionVisitor.visitBitStringConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNullConstant(@Nonnull RelationalParser.NullConstantContext ctx) {
        return expressionVisitor.visitNullConstant(ctx);
    }

    @Nonnull
    @Override
    public Object visitStringDataType(@Nonnull RelationalParser.StringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitNationalStringDataType(@Nonnull RelationalParser.NationalStringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitNationalVaryingStringDataType(@Nonnull RelationalParser.NationalVaryingStringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitDimensionDataType(@Nonnull RelationalParser.DimensionDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSimpleDataType(@Nonnull RelationalParser.SimpleDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitCollectionDataType(@Nonnull RelationalParser.CollectionDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSpatialDataType(@Nonnull RelationalParser.SpatialDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLongVarcharDataType(@Nonnull RelationalParser.LongVarcharDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLongVarbinaryDataType(@Nonnull RelationalParser.LongVarbinaryDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitCollectionOptions(@Nonnull RelationalParser.CollectionOptionsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitConvertedDataType(@Nonnull RelationalParser.ConvertedDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLengthOneDimension(@Nonnull RelationalParser.LengthOneDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLengthTwoDimension(@Nonnull RelationalParser.LengthTwoDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLengthTwoOptionalDimension(@Nonnull RelationalParser.LengthTwoOptionalDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public List<Identifier> visitUidList(@Nonnull RelationalParser.UidListContext ctx) {
        return identifierVisitor.visitUidList(ctx);
    }

    @Nonnull
    @Override
    public Object visitUidWithNestings(@Nonnull RelationalParser.UidWithNestingsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestingsInParens(@Nonnull RelationalParser.UidListWithNestingsInParensContext ctx) {
        return expressionVisitor.visitUidListWithNestingsInParens(ctx);
    }

    @Nonnull
    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestings(@Nonnull RelationalParser.UidListWithNestingsContext ctx) {
        return expressionVisitor.visitUidListWithNestings(ctx);
    }

    @Nonnull
    @Override
    public Object visitTables(@Nonnull RelationalParser.TablesContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitIndexColumnNames(@Nonnull RelationalParser.IndexColumnNamesContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitExpressions(@Nonnull RelationalParser.ExpressionsContext ctx) {
        return expressionVisitor.visitExpressions(ctx);
    }

    @Nonnull
    @Override
    public Object visitExpressionsWithDefaults(@Nonnull RelationalParser.ExpressionsWithDefaultsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructorForInsert(@Nonnull RelationalParser.RecordConstructorForInsertContext ctx) {
        return expressionVisitor.visitRecordConstructorForInsert(ctx);
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructorForInlineTable(final RelationalParser.RecordConstructorForInlineTableContext ctx) {
        return expressionVisitor.visitRecordConstructorForInlineTable(ctx);
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructor(@Nonnull RelationalParser.RecordConstructorContext ctx) {
        return expressionVisitor.visitRecordConstructor(ctx);
    }

    @Nonnull
    @Override
    public Object visitOfTypeClause(@Nonnull RelationalParser.OfTypeClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitArrayConstructor(@Nonnull RelationalParser.ArrayConstructorContext ctx) {
        return expressionVisitor.visitArrayConstructor(ctx);
    }

    @Nonnull
    @Override
    public Object visitUserVariables(@Nonnull RelationalParser.UserVariablesContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitDefaultValue(@Nonnull RelationalParser.DefaultValueContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitCurrentTimestamp(@Nonnull RelationalParser.CurrentTimestampContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitExpressionOrDefault(@Nonnull RelationalParser.ExpressionOrDefaultContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitExpressionWithName(@Nonnull RelationalParser.ExpressionWithNameContext ctx) {
        return expressionVisitor.visitExpressionWithName(ctx);
    }

    @Nonnull
    @Override
    public Expression visitExpressionWithOptionalName(@Nonnull RelationalParser.ExpressionWithOptionalNameContext ctx) {
        return expressionVisitor.visitExpressionWithOptionalName(ctx);
    }

    @Nonnull
    @Override
    public Object visitIfExists(@Nonnull RelationalParser.IfExistsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitIfNotExists(@Nonnull RelationalParser.IfNotExistsContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitAggregateFunctionCall(@Nonnull RelationalParser.AggregateFunctionCallContext ctx) {
        return expressionVisitor.visitAggregateFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitSpecificFunctionCall(@Nonnull RelationalParser.SpecificFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx) {
        return expressionVisitor.visitScalarFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Expression visitUserDefinedScalarFunctionCall(@Nonnull RelationalParser.UserDefinedScalarFunctionCallContext ctx) {
        return expressionVisitor.visitUserDefinedScalarFunctionCall(ctx);
    }


    @Nonnull
    @Override
    public Object visitUserDefinedScalarFunctionName(@Nonnull RelationalParser.UserDefinedScalarFunctionNameContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSimpleFunctionCall(@Nonnull RelationalParser.SimpleFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitDataTypeFunctionCall(@Nonnull RelationalParser.DataTypeFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitValuesFunctionCall(@Nonnull RelationalParser.ValuesFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitCaseExpressionFunctionCall(@Nonnull RelationalParser.CaseExpressionFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitCaseFunctionCall(@Nonnull RelationalParser.CaseFunctionCallContext ctx) {
        return expressionVisitor.visitCaseFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitCharFunctionCall(@Nonnull RelationalParser.CharFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitPositionFunctionCall(@Nonnull RelationalParser.PositionFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitSubstrFunctionCall(@Nonnull RelationalParser.SubstrFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitTrimFunctionCall(@Nonnull RelationalParser.TrimFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitWeightFunctionCall(@Nonnull RelationalParser.WeightFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitExtractFunctionCall(@Nonnull RelationalParser.ExtractFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitGetFormatFunctionCall(@Nonnull RelationalParser.GetFormatFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitCaseFuncAlternative(@Nonnull RelationalParser.CaseFuncAlternativeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLevelWeightList(@Nonnull RelationalParser.LevelWeightListContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLevelWeightRange(@Nonnull RelationalParser.LevelWeightRangeContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLevelInWeightListElement(@Nonnull RelationalParser.LevelInWeightListElementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitAggregateWindowedFunction(@Nonnull RelationalParser.AggregateWindowedFunctionContext ctx) {
        return expressionVisitor.visitAggregateWindowedFunction(ctx);
    }

    @Nonnull
    @Override
    public Object visitNonAggregateWindowedFunction(@Nonnull RelationalParser.NonAggregateWindowedFunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitOverClause(@Nonnull RelationalParser.OverClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitWindowName(@Nonnull RelationalParser.WindowNameContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitScalarFunctionName(@Nonnull RelationalParser.ScalarFunctionNameContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitFunctionArgs(@Nonnull RelationalParser.FunctionArgsContext ctx) {
        return expressionVisitor.visitFunctionArgs(ctx);
    }

    @Nonnull
    @Override
    public Object visitFunctionArg(@Nonnull RelationalParser.FunctionArgContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitNamedFunctionArg(final RelationalParser.NamedFunctionArgContext ctx) {
        return expressionVisitor.visitNamedFunctionArg(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNotExpression(@Nonnull RelationalParser.NotExpressionContext ctx) {
        return expressionVisitor.visitNotExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitLogicalExpression(@Nonnull RelationalParser.LogicalExpressionContext ctx) {
        return expressionVisitor.visitLogicalExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitPredicatedExpression(@Nonnull RelationalParser.PredicatedExpressionContext ctx) {
        return expressionVisitor.visitPredicatedExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBinaryComparisonPredicate(@Nonnull RelationalParser.BinaryComparisonPredicateContext ctx) {
        return expressionVisitor.visitBinaryComparisonPredicate(ctx);
    }

    @Override
    public Expression visitSubscriptExpression(@Nonnull RelationalParser.SubscriptExpressionContext ctx) {
        return expressionVisitor.visitSubscriptExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitInList(@Nonnull RelationalParser.InListContext ctx) {
        return expressionVisitor.visitInList(ctx);
    }

    @Nonnull
    @Override
    public Object visitConstantExpressionAtom(@Nonnull RelationalParser.ConstantExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitFunctionCallExpressionAtom(@Nonnull RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return expressionVisitor.visitFunctionCallExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Object visitFullColumnNameExpressionAtom(@Nonnull RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBitExpressionAtom(@Nonnull RelationalParser.BitExpressionAtomContext ctx) {
        return expressionVisitor.visitBitExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitPreparedStatementParameterAtom(@Nonnull RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return expressionVisitor.visitPreparedStatementParameterAtom(ctx);
    }

    @Nonnull
    @Override
    public Object visitRecordConstructorExpressionAtom(@Nonnull RelationalParser.RecordConstructorExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitArrayConstructorExpressionAtom(@Nonnull RelationalParser.ArrayConstructorExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Expression visitMathExpressionAtom(@Nonnull RelationalParser.MathExpressionAtomContext ctx) {
        return expressionVisitor.visitMathExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitExistsExpressionAtom(@Nonnull RelationalParser.ExistsExpressionAtomContext ctx) {
        return expressionVisitor.visitExistsExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx) {
        return expressionVisitor.visitPreparedStatementParameter(ctx);
    }

    @Nonnull
    @Override
    public Object visitUnaryOperator(@Nonnull RelationalParser.UnaryOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitComparisonOperator(@Nonnull RelationalParser.ComparisonOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitLogicalOperator(@Nonnull RelationalParser.LogicalOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitBitOperator(@Nonnull RelationalParser.BitOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitMathOperator(@Nonnull RelationalParser.MathOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitJsonOperator(@Nonnull RelationalParser.JsonOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitCharsetNameBase(@Nonnull RelationalParser.CharsetNameBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitIntervalTypeBase(@Nonnull RelationalParser.IntervalTypeBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitKeywordsCanBeId(@Nonnull RelationalParser.KeywordsCanBeIdContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitFunctionNameBase(@Nonnull RelationalParser.FunctionNameBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    @Override
    public Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Nonnull
    public DdlQueryFactory getDdlQueryFactory() {
        return ddlQueryFactory;
    }

    @Nonnull
    public URI getDbUri() {
        return dbUri;
    }
}
