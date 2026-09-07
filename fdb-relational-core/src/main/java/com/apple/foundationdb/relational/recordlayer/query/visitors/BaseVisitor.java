/*
 * BaseVisitor.java
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
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
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
import com.apple.foundationdb.relational.recordlayer.query.WindowSpecExpression;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import com.apple.foundationdb.relational.recordlayer.query.functions.SqlFunctionCatalog;
import com.apple.foundationdb.relational.util.Assert;
import org.antlr.v4.runtime.tree.ParseTree;
import org.jspecify.annotations.Nullable;
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

    protected final MutablePlanGenerationContext mutablePlanGenerationContext;

    private final DdlQueryFactory ddlQueryFactory;

    private final URI dbUri;

    /**
     * The current plan fragment. This will initially be empty, prior to the first call to {@link #pushPlanFragment()}.
     */
    protected Optional<LogicalPlanFragment> currentPlanFragment;

    private final ExpressionVisitor expressionVisitor;

    private final IdentifierVisitor identifierVisitor;

    private final QueryVisitor queryVisitor;

    private final MetadataPlanVisitor metadataPlanVisitor;

    private final DdlVisitor ddlVisitor;

    private RecordLayerSchemaTemplate metadata;

    private SemanticAnalyzer semanticAnalyzer;

    private final LogicalOperatorCatalog logicalOperatorCatalog;

    @SuppressWarnings("this-escape")
    public BaseVisitor(MutablePlanGenerationContext mutablePlanGenerationContext,
                       RecordLayerSchemaTemplate metadata,
                       DdlQueryFactory ddlQueryFactory,
                       MetadataOperationsFactory metadataOperationsFactory,
                       URI dbUri,
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
        this.semanticAnalyzer = new SemanticAnalyzer(getSchemaTemplate(), createFunctionCatalog(getSchemaTemplate()),
                mutablePlanGenerationContext, isCaseSensitive());
        this.logicalOperatorCatalog = LogicalOperatorCatalog.newInstance();
    }

    public MutablePlanGenerationContext getPlanGenerationContext() {
        return mutablePlanGenerationContext;
    }

    protected IdentifierVisitor getIdentifierVisitor() {
        return identifierVisitor;
    }

    public Plan<?> generateLogicalPlan(ParseTree parseTree) {
        final var result = visit(parseTree);
        return Assert.castUnchecked(result, Plan.class, ErrorCode.INTERNAL_ERROR, () -> "Could not generate a logical plan");
    }

    public RecordLayerSchemaTemplate getSchemaTemplate() {
        return metadata;
    }

    public RecordLayerSchemaTemplate replaceSchemaTemplate(RecordLayerSchemaTemplate newCatalog) {
        final var oldMetadata = metadata;
        metadata = newCatalog;
        semanticAnalyzer = new SemanticAnalyzer(metadata, createFunctionCatalog(metadata), mutablePlanGenerationContext, isCaseSensitive());
        return oldMetadata;
    }

    public SemanticAnalyzer getSemanticAnalyzer() {
        return semanticAnalyzer;
    }

    public LogicalOperatorCatalog getLogicalOperatorCatalog() {
        return logicalOperatorCatalog;
    }

    public SqlFunctionCatalog createFunctionCatalog(final RecordLayerSchemaTemplate metadata) {
        return SqlFunctionCatalog.newInstance(metadata, isCaseSensitive());
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    LogicalOperators getLogicalOperators() {
        return currentPlanFragment.orElseThrow().getLogicalOperators();
    }

    LogicalOperators getLogicalOperatorsIncludingOuter() {
        return currentPlanFragment.orElseThrow().getLogicalOperatorsIncludingOuter();
    }

    public boolean isTopLevel() {
        return !(currentPlanFragment.isPresent() && currentPlanFragment.get().hasParent());
    }

    /**
     * Enters a new SQL scope by pushing a fresh {@link LogicalPlanFragment} onto the stack. If a fragment already
     * exists, it becomes the parent of the new fragment (enabling outer-correlation resolution); otherwise a root
     * fragment is created. Every call must be balanced by a corresponding {@link #popPlanFragment()}.
     *
     * <p>Typical call sites: {@code visitSimpleTable} (every {@code SELECT} block), {@code visitQuery} (CTEs and
     * subqueries), and table-access visitors that set up an initial operator.
     *
     * @return the newly created (now current) fragment
     *
     * @see #popPlanFragment
     */
    public LogicalPlanFragment pushPlanFragment() {
        currentPlanFragment = Optional.of(currentPlanFragment.map(LogicalPlanFragment::addChild).orElse(LogicalPlanFragment.ofRoot()));
        return currentPlanFragment.get();
    }

    /**
     * Leaves the current SQL scope by popping the top fragment off the stack. The parent fragment (if any) becomes
     * current again. After a pop, any operators or predicates accumulated in the popped fragment are no longer directly
     * accessible; the caller is expected to have already folded them into a relational expression before popping.
     *
     * @see #pushPlanFragment
     */
    public void popPlanFragment() {
        this.currentPlanFragment = currentPlanFragment.flatMap(LogicalPlanFragment::getParentMaybe);
    }

    /**
     * Returns the plan fragment for the SQL scope currently being translated.
     *
     * @throws com.apple.foundationdb.relational.api.exceptions.RelationalException if no fragment has been pushed
     * @see #pushPlanFragment
     */
    LogicalPlanFragment getCurrentPlanFragment() {
        Assert.thatUnchecked(currentPlanFragment.isPresent());
        return currentPlanFragment.get();
    }

    Optional<LogicalPlanFragment> getCurrentPlanFragmentMaybe() {
        return currentPlanFragment;
    }

    boolean isForDdl() {
        return getPlanGenerationContext().isForDdl();
    }

    // SemanticAnalyzer.normalizeString() is @Nullable only when its input is null; value here is @NonNull, so the
    // result is never null. Assert.notNullUnchecked enforces that invariant at runtime with a clear
    // RelationalException, but NullAway can't see that since Assert lives in the not-yet-migrated
    // fdb-relational-api module.
    @SuppressWarnings("NullAway")
    protected String normalizeString(final String value) {
        return Assert.notNullUnchecked(SemanticAnalyzer.normalizeString(value, caseSensitive));
    }

    public Expression resolveFunction(String functionName, Expression... arguments) {
        return resolveFunction(functionName, Expressions.of(arguments));
    }

    public Expression resolveFunction(String functionName, Expressions arguments) {
        return getSemanticAnalyzer().resolveFunction(functionName, arguments.toCallSiteArguments(true), true);
    }

    public Expression resolveFunction(String functionName, boolean flattenSingleItemRecords, Expression... arguments) {
        return getSemanticAnalyzer().resolveFunction(functionName,
                Expressions.of(arguments).toCallSiteArguments(flattenSingleItemRecords), flattenSingleItemRecords);
    }

    public LogicalOperator resolveTableValuedFunction(Identifier functionName, Expressions arguments) {
        return getSemanticAnalyzer().resolveTableFunction(functionName, arguments, true);
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        return nextResult != null ? nextResult : aggregate;
    }

    @Override
    public Object visitRoot(RelationalParser.RootContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitStatements(RelationalParser.StatementsContext ctx) {
        return visitChildren(ctx);
    }

    @Nullable
    @Override
    public Object visitStatement(RelationalParser.StatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public QueryPlan.LogicalQueryPlan visitDmlStatement(RelationalParser.DmlStatementContext ctx) {
        return queryVisitor.visitDmlStatement(ctx);
    }

    @Override
    public Object visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTransactionStatement(RelationalParser.TransactionStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPreparedStatement(RelationalParser.PreparedStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitAdministrationStatement(RelationalParser.AdministrationStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitUtilityStatement(RelationalParser.UtilityStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTemplateClause(RelationalParser.TemplateClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public ProceduralPlan visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx) {
        return ddlVisitor.visitCreateSchemaStatement(ctx);
    }

    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        return ddlVisitor.visitCreateSchemaTemplateStatement(ctx);
    }

    @Override
    public ProceduralPlan visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
        return ddlVisitor.visitCreateDatabaseStatement(ctx);
    }

    @Override
    public Object visitOptionsClause(RelationalParser.OptionsClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitOption(RelationalParser.OptionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public ProceduralPlan visitDropDatabaseStatement(RelationalParser.DropDatabaseStatementContext ctx) {
        return ddlVisitor.visitDropDatabaseStatement(ctx);
    }

    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return ddlVisitor.visitDropSchemaTemplateStatement(ctx);
    }

    @Override
    public ProceduralPlan visitDropSchemaStatement(RelationalParser.DropSchemaStatementContext ctx) {
        return ddlVisitor.visitDropSchemaStatement(ctx);
    }

    @Override
    public RecordLayerTable visitStructDefinition(RelationalParser.StructDefinitionContext ctx) {
        return ddlVisitor.visitStructDefinition(ctx);
    }

    @Override
    public RecordLayerTable visitTableDefinition(RelationalParser.TableDefinitionContext ctx) {
        return ddlVisitor.visitTableDefinition(ctx);
    }

    @Override
    public Object visitColumnDefinition(RelationalParser.ColumnDefinitionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public DataType visitFunctionColumnType(final RelationalParser.FunctionColumnTypeContext ctx) {
        return ddlVisitor.visitFunctionColumnType(ctx);
    }

    @Override
    public Boolean visitNullColumnConstraint(RelationalParser.NullColumnConstraintContext ctx) {
        return ddlVisitor.visitNullColumnConstraint(ctx);
    }

    @Override
    public Object visitPrimaryKeyDefinition(RelationalParser.PrimaryKeyDefinitionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public List<Identifier> visitFullIdList(RelationalParser.FullIdListContext ctx) {
        return identifierVisitor.visitFullIdList(ctx);
    }

    @Override
    public DataType.Named visitEnumDefinition(RelationalParser.EnumDefinitionContext ctx) {
        return ddlVisitor.visitEnumDefinition(ctx);
    }

    @Override
    public RecordLayerIndex visitIndexAsSelectDefinition(RelationalParser.IndexAsSelectDefinitionContext ctx) {
        return ddlVisitor.visitIndexAsSelectDefinition(ctx);
    }

    @Override
    public RecordLayerIndex visitIndexOnSourceDefinition(RelationalParser.IndexOnSourceDefinitionContext ctx) {
        return ddlVisitor.visitIndexOnSourceDefinition(ctx);
    }

    @Override
    public RecordLayerIndex visitVectorIndexDefinition(final RelationalParser.VectorIndexDefinitionContext ctx) {
        return ddlVisitor.visitVectorIndexDefinition(ctx);
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
    public CompiledSqlFunction visitTempSqlInvokedFunction(final RelationalParser.TempSqlInvokedFunctionContext ctx) {
        return ddlVisitor.visitTempSqlInvokedFunction(ctx);
    }

    @Override
    public UserDefinedFunction visitSqlInvokedFunction(RelationalParser.SqlInvokedFunctionContext ctx) {
        return ddlVisitor.visitSqlInvokedFunction(ctx);
    }

    @Override
    public LogicalOperator visitStatementBody(final RelationalParser.StatementBodyContext ctx) {
        return ddlVisitor.visitStatementBody(ctx);
    }

    @Override
    public Expression visitUserDefinedMacroFunctionStatementBody(RelationalParser.UserDefinedMacroFunctionStatementBodyContext ctx) {
        return ddlVisitor.visitUserDefinedMacroFunctionStatementBody(ctx);
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
    public Object visitCharSet(RelationalParser.CharSetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitIntervalType(RelationalParser.IntervalTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSchemaId(RelationalParser.SchemaIdContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPath(RelationalParser.PathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSchemaTemplateId(RelationalParser.SchemaTemplateIdContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public LogicalOperator visitDeleteStatement(RelationalParser.DeleteStatementContext ctx) {
        return queryVisitor.visitDeleteStatement(ctx);
    }

    @Override
    public LogicalOperator visitInsertStatement(RelationalParser.InsertStatementContext ctx) {
        return queryVisitor.visitInsertStatement(ctx);
    }

    @Override
    public QueryPlan.LogicalQueryPlan visitSelectStatement(RelationalParser.SelectStatementContext ctx) {
        return queryVisitor.visitSelectStatement(ctx);
    }

    @Override
    public LogicalOperator visitQuery(RelationalParser.QueryContext ctx) {
        return queryVisitor.visitQuery(ctx);
    }

    @Nullable
    @Override
    public Void visitCtes(RelationalParser.CtesContext ctx) {
        return queryVisitor.visitCtes(ctx);
    }

    @Override
    public LogicalOperator visitNamedQuery(RelationalParser.NamedQueryContext ctx) {
        return queryVisitor.visitNamedQuery(ctx);
    }

    @Override
    public LogicalOperator visitTableFunction(final RelationalParser.TableFunctionContext ctx) {
        return expressionVisitor.visitTableFunction(ctx);
    }

    @Override
    public Expressions visitNamedOrUnnamedFunctionArgs(RelationalParser.NamedOrUnnamedFunctionArgsContext ctx) {
        return expressionVisitor.visitNamedOrUnnamedFunctionArgs(ctx);
    }

    @Override
    public Identifier visitTableFunctionName(final RelationalParser.TableFunctionNameContext ctx) {
        return identifierVisitor.visitTableFunctionName(ctx);
    }

    @Override
    public Expression visitContinuationAtom(RelationalParser.ContinuationAtomContext ctx) {
        return expressionVisitor.visitContinuationAtom(ctx);
    }

    @Override
    public LogicalOperator visitQueryTermDefault(RelationalParser.QueryTermDefaultContext ctx) {
        return Assert.castUnchecked(visitChildren(ctx), LogicalOperator.class);
    }

    @Override
    public LogicalOperator visitSetQuery(RelationalParser.SetQueryContext ctx) {
        return queryVisitor.visitSetQuery(ctx);
    }

    @Override
    public LogicalOperator visitInsertStatementValueSelect(RelationalParser.InsertStatementValueSelectContext ctx) {
        return queryVisitor.visitInsertStatementValueSelect(ctx);
    }

    @Override
    public LogicalOperator visitInsertStatementValueValues(RelationalParser.InsertStatementValueValuesContext ctx) {
        return queryVisitor.visitInsertStatementValueValues(ctx);
    }

    @Override
    public Expressions visitUpdatedElement(RelationalParser.UpdatedElementContext ctx) {
        return expressionVisitor.visitUpdatedElement(ctx);
    }

    @Override
    public Object visitAssignmentField(RelationalParser.AssignmentFieldContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public LogicalOperator visitUpdateStatement(RelationalParser.UpdateStatementContext ctx) {
        return queryVisitor.visitUpdateStatement(ctx);
    }

    @Override
    public List<OrderByExpression> visitOrderByClause(RelationalParser.OrderByClauseContext ctx) {
        return expressionVisitor.visitOrderByClause(ctx);
    }

    @Override
    public OrderByExpression visitOrderByExpression(RelationalParser.OrderByExpressionContext ctx) {
        return expressionVisitor.visitOrderByExpression(ctx);
    }

    @Override
    @Nullable
    public Void visitTableSources(RelationalParser.TableSourcesContext ctx) {
        return queryVisitor.visitTableSources(ctx);
    }

    @Nullable
    @Override
    public Void visitTableSourceBase(RelationalParser.TableSourceBaseContext ctx) {
        return queryVisitor.visitTableSourceBase(ctx);
    }

    @Override
    public LogicalOperator visitAtomTableItem(RelationalParser.AtomTableItemContext ctx) {
        return queryVisitor.visitAtomTableItem(ctx);
    }

    @Override
    public LogicalOperator visitSubqueryTableItem(RelationalParser.SubqueryTableItemContext ctx) {
        return queryVisitor.visitSubqueryTableItem(ctx);
    }

    @Override
    public LogicalOperator visitInlineTableItem(RelationalParser.InlineTableItemContext ctx) {
        return queryVisitor.visitInlineTableItem(ctx);
    }

    @Override
    public LogicalOperator visitTableValuedFunction(final RelationalParser.TableValuedFunctionContext ctx) {
        return queryVisitor.visitTableValuedFunction(ctx);
    }

    @Override
    public Set<String> visitIndexHint(RelationalParser.IndexHintContext ctx) {
        return queryVisitor.visitIndexHint(ctx);
    }

    @Override
    public Object visitIndexHintType(RelationalParser.IndexHintTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> visitInlineTableDefinition(final RelationalParser.InlineTableDefinitionContext ctx) {
        return expressionVisitor.visitInlineTableDefinition(ctx);
    }

    @Nullable
    @Override
    public Object visitInnerJoin(RelationalParser.InnerJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitStraightJoin(RelationalParser.StraightJoinContext ctx) {
        throw new RelationalException("STRAIGHT_JOIN is not supported", ErrorCode.UNSUPPORTED_QUERY).toUncheckedWrappedException();
    }

    @Override
    public Object visitOuterJoin(RelationalParser.OuterJoinContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitNaturalJoin(RelationalParser.NaturalJoinContext ctx) {
        throw new RelationalException("NATURAL JOIN is not supported", ErrorCode.UNSUPPORTED_QUERY).toUncheckedWrappedException();
    }

    @Override
    public LogicalOperator visitSimpleTable(RelationalParser.SimpleTableContext ctx) {
        return queryVisitor.visitSimpleTable(ctx);
    }

    @Override
    public LogicalOperator visitParenthesisQuery(RelationalParser.ParenthesisQueryContext ctx) {
        return queryVisitor.visitParenthesisQuery(ctx);
    }

    @Override
    public Expressions visitSelectElements(RelationalParser.SelectElementsContext ctx) {
        return expressionVisitor.visitSelectElements(ctx);
    }

    @Override
    public Expression visitSelectStarElement(RelationalParser.SelectStarElementContext ctx) {
        return expressionVisitor.visitSelectStarElement(ctx);
    }

    @Override
    public Object visitSelectQualifierStarElement(RelationalParser.SelectQualifierStarElementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext ctx) {
        return expressionVisitor.visitSelectExpressionElement(ctx);
    }

    @Override
    @Nullable
    public Void visitFromClause(RelationalParser.FromClauseContext ctx) {
        return queryVisitor.visitFromClause(ctx);
    }

    @Override
    public Expressions visitGroupByClause(RelationalParser.GroupByClauseContext ctx) {
        return expressionVisitor.visitGroupByClause(ctx);
    }

    @Override
    public Expression visitWhereExpr(RelationalParser.WhereExprContext ctx) {
        return expressionVisitor.visitWhereExpr(ctx);
    }

    @Override
    public Expression visitHavingClause(RelationalParser.HavingClauseContext ctx) {
        return expressionVisitor.visitHavingClause(ctx);
    }

    @Override
    public Expression visitQualifyClause(final RelationalParser.QualifyClauseContext ctx) {
        return expressionVisitor.visitQualifyClause(ctx);
    }

    @Override
    public Expression visitGroupByItem(RelationalParser.GroupByItemContext ctx) {
        return expressionVisitor.visitGroupByItem(ctx);
    }

    @Override
    public Expression visitLimitClause(RelationalParser.LimitClauseContext ctx) {
        return expressionVisitor.visitLimitClause(ctx);
    }

    @Override
    public Expression visitLimitClauseAtom(RelationalParser.LimitClauseAtomContext ctx) {
        return expressionVisitor.visitLimitClauseAtom(ctx);
    }

    @Override
    public Object visitStatementOptions(RelationalParser.StatementOptionsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitStatementOption(RelationalParser.StatementOptionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitStartTransaction(RelationalParser.StartTransactionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitCommitStatement(RelationalParser.CommitStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitRollbackStatement(RelationalParser.RollbackStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSetAutocommitStatement(RelationalParser.SetAutocommitStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSetTransactionStatement(RelationalParser.SetTransactionStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTransactionOption(RelationalParser.TransactionOptionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTransactionLevel(RelationalParser.TransactionLevelContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPrepareStatement(RelationalParser.PrepareStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitExecuteStatement(RelationalParser.ExecuteStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(RelationalParser.ShowDatabasesStatementContext ctx) {
        return metadataPlanVisitor.visitShowDatabasesStatement(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        return metadataPlanVisitor.visitShowSchemaTemplatesStatement(ctx);
    }

    @Override
    public Object visitSetVariable(RelationalParser.SetVariableContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSetCharset(RelationalParser.SetCharsetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSetNames(RelationalParser.SetNamesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSetTransaction(RelationalParser.SetTransactionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSetAutocommit(RelationalParser.SetAutocommitContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSetNewValueInsideTrigger(RelationalParser.SetNewValueInsideTriggerContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitVariableClause(RelationalParser.VariableClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitKillStatement(RelationalParser.KillStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitResetStatement(RelationalParser.ResetStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTableIndexes(RelationalParser.TableIndexesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLoadedTableIndexes(RelationalParser.LoadedTableIndexesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        return metadataPlanVisitor.visitSimpleDescribeSchemaStatement(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        return metadataPlanVisitor.visitSimpleDescribeSchemaTemplateStatement(ctx);
    }

    @Override
    public QueryPlan.LogicalQueryPlan visitFullDescribeStatement(RelationalParser.FullDescribeStatementContext ctx) {
        return queryVisitor.visitFullDescribeStatement(ctx);
    }

    @Override
    public Object visitHelpStatement(RelationalParser.HelpStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitDescribeStatements(RelationalParser.DescribeStatementsContext ctx) {
        return queryVisitor.visitDescribeStatements(ctx);
    }

    @Override
    public Object visitDescribeConnection(RelationalParser.DescribeConnectionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Identifier visitFullId(RelationalParser.FullIdContext ctx) {
        return identifierVisitor.visitFullId(ctx);
    }

    @Override
    public Identifier visitTableName(RelationalParser.TableNameContext ctx) {
        return identifierVisitor.visitTableName(ctx);
    }

    @Override
    public Expression visitFullColumnName(RelationalParser.FullColumnNameContext ctx) {
        return expressionVisitor.visitFullColumnName(ctx);
    }

    @Override
    public Identifier visitIndexColumnName(RelationalParser.IndexColumnNameContext ctx) {
        return identifierVisitor.visitIndexColumnName(ctx);
    }

    @Override
    public Identifier visitCharsetName(RelationalParser.CharsetNameContext ctx) {
        return identifierVisitor.visitCharsetName(ctx);
    }

    @Override
    public Identifier visitCollationName(RelationalParser.CollationNameContext ctx) {
        return identifierVisitor.visitCollationName(ctx);
    }

    @Override
    public Identifier visitUid(RelationalParser.UidContext ctx) {
        return identifierVisitor.visitUid(ctx);
    }

    @Override
    public Identifier visitSimpleId(RelationalParser.SimpleIdContext ctx) {
        return identifierVisitor.visitSimpleId(ctx);
    }

    @Override
    public Object visitNullNotnull(RelationalParser.NullNotnullContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitDecimalLiteral(RelationalParser.DecimalLiteralContext ctx) {
        return expressionVisitor.visitDecimalLiteral(ctx);
    }

    @Override
    public Expression visitStringLiteral(RelationalParser.StringLiteralContext ctx) {
        return expressionVisitor.visitStringLiteral(ctx);
    }

    @Override
    public Expression visitBooleanLiteral(RelationalParser.BooleanLiteralContext ctx) {
        return expressionVisitor.visitBooleanLiteral(ctx);
    }

    @Override
    public Expression visitBytesLiteral(RelationalParser.BytesLiteralContext ctx) {
        return expressionVisitor.visitBytesLiteral(ctx);
    }

    @Override
    public Expression visitNullLiteral(RelationalParser.NullLiteralContext ctx) {
        return expressionVisitor.visitNullLiteral(ctx);
    }

    @Override
    public Expression visitStringConstant(RelationalParser.StringConstantContext ctx) {
        return expressionVisitor.visitStringConstant(ctx);
    }

    @Override
    public Expression visitDecimalConstant(RelationalParser.DecimalConstantContext ctx) {
        return expressionVisitor.visitDecimalConstant(ctx);
    }

    @Override
    public Expression visitNegativeDecimalConstant(RelationalParser.NegativeDecimalConstantContext ctx) {
        return expressionVisitor.visitNegativeDecimalConstant(ctx);
    }

    @Override
    public Expression visitBytesConstant(RelationalParser.BytesConstantContext ctx) {
        return expressionVisitor.visitBytesConstant(ctx);
    }

    @Override
    public Expression visitBooleanConstant(RelationalParser.BooleanConstantContext ctx) {
        return expressionVisitor.visitBooleanConstant(ctx);
    }

    @Override
    public Expression visitBitStringConstant(RelationalParser.BitStringConstantContext ctx) {
        return expressionVisitor.visitBitStringConstant(ctx);
    }

    @Override
    public Expression visitNullConstant(RelationalParser.NullConstantContext ctx) {
        return expressionVisitor.visitNullConstant(ctx);
    }

    @Override
    public Object visitStringDataType(RelationalParser.StringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitNationalStringDataType(RelationalParser.NationalStringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitNationalVaryingStringDataType(RelationalParser.NationalVaryingStringDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitDimensionDataType(RelationalParser.DimensionDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSimpleDataType(RelationalParser.SimpleDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitCollectionDataType(RelationalParser.CollectionDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSpatialDataType(RelationalParser.SpatialDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLongVarcharDataType(RelationalParser.LongVarcharDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLongVarbinaryDataType(RelationalParser.LongVarbinaryDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitCollectionOptions(RelationalParser.CollectionOptionsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitConvertedDataType(RelationalParser.ConvertedDataTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLengthOneDimension(RelationalParser.LengthOneDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLengthTwoDimension(RelationalParser.LengthTwoDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLengthTwoOptionalDimension(RelationalParser.LengthTwoOptionalDimensionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public List<Identifier> visitUidList(RelationalParser.UidListContext ctx) {
        return identifierVisitor.visitUidList(ctx);
    }

    @Override
    public Object visitUidWithNestings(RelationalParser.UidWithNestingsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestingsInParens(RelationalParser.UidListWithNestingsInParensContext ctx) {
        return expressionVisitor.visitUidListWithNestingsInParens(ctx);
    }

    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestings(RelationalParser.UidListWithNestingsContext ctx) {
        return expressionVisitor.visitUidListWithNestings(ctx);
    }

    @Override
    public Object visitTables(RelationalParser.TablesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitIndexColumnNames(RelationalParser.IndexColumnNamesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expressions visitExpressions(RelationalParser.ExpressionsContext ctx) {
        return expressionVisitor.visitExpressions(ctx);
    }

    @Override
    public Object visitExpressionsWithDefaults(RelationalParser.ExpressionsWithDefaultsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitRecordConstructorForInsert(RelationalParser.RecordConstructorForInsertContext ctx) {
        return expressionVisitor.visitRecordConstructorForInsert(ctx);
    }

    @Override
    public Expression visitRecordConstructorForInlineTable(final RelationalParser.RecordConstructorForInlineTableContext ctx) {
        return expressionVisitor.visitRecordConstructorForInlineTable(ctx);
    }

    @Override
    public Expression visitRecordConstructor(RelationalParser.RecordConstructorContext ctx) {
        return expressionVisitor.visitRecordConstructor(ctx);
    }

    @Override
    public Object visitOfTypeClause(RelationalParser.OfTypeClauseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitArrayConstructor(RelationalParser.ArrayConstructorContext ctx) {
        return expressionVisitor.visitArrayConstructor(ctx);
    }

    @Override
    public Object visitUserVariables(RelationalParser.UserVariablesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitDefaultValue(RelationalParser.DefaultValueContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitCurrentTimestamp(RelationalParser.CurrentTimestampContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitExpressionOrDefault(RelationalParser.ExpressionOrDefaultContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitExpressionWithOptionalName(RelationalParser.ExpressionWithOptionalNameContext ctx) {
        return expressionVisitor.visitExpressionWithOptionalName(ctx);
    }

    @Nullable
    @Override
    public Object visitIfExists(RelationalParser.IfExistsContext ctx) {
        return visitChildren(ctx);
    }

    @Nullable
    @Override
    public Object visitIfNotExists(RelationalParser.IfNotExistsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitAggregateFunctionCall(RelationalParser.AggregateFunctionCallContext ctx) {
        return expressionVisitor.visitAggregateFunctionCall(ctx);
    }

    @Override
    public Object visitSpecificFunctionCall(RelationalParser.SpecificFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitScalarFunctionCall(RelationalParser.ScalarFunctionCallContext ctx) {
        return expressionVisitor.visitScalarFunctionCall(ctx);
    }

    @Override
    public Expression visitUserDefinedScalarFunctionCall(RelationalParser.UserDefinedScalarFunctionCallContext ctx) {
        return expressionVisitor.visitUserDefinedScalarFunctionCall(ctx);
    }

    @Override
    public Object visitSimpleFunctionCall(RelationalParser.SimpleFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitDataTypeFunctionCall(RelationalParser.DataTypeFunctionCallContext ctx) {
        return expressionVisitor.visitDataTypeFunctionCall(ctx);
    }

    @Override
    public Object visitValuesFunctionCall(RelationalParser.ValuesFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitCaseExpressionFunctionCall(RelationalParser.CaseExpressionFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitCaseFunctionCall(RelationalParser.CaseFunctionCallContext ctx) {
        return expressionVisitor.visitCaseFunctionCall(ctx);
    }

    @Override
    public Object visitCharFunctionCall(RelationalParser.CharFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPositionFunctionCall(RelationalParser.PositionFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSubstrFunctionCall(RelationalParser.SubstrFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitTrimFunctionCall(RelationalParser.TrimFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitWeightFunctionCall(RelationalParser.WeightFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitExtractFunctionCall(RelationalParser.ExtractFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitGetFormatFunctionCall(RelationalParser.GetFormatFunctionCallContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitCaseFuncAlternative(RelationalParser.CaseFuncAlternativeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLevelWeightList(RelationalParser.LevelWeightListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLevelWeightRange(RelationalParser.LevelWeightRangeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLevelInWeightListElement(RelationalParser.LevelInWeightListElementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitAggregateWindowedFunction(RelationalParser.AggregateWindowedFunctionContext ctx) {
        return expressionVisitor.visitAggregateWindowedFunction(ctx);
    }

    @Override
    public Boolean visitNullTreatmentClause(RelationalParser.NullTreatmentClauseContext ctx) {
        return expressionVisitor.visitNullTreatmentClause(ctx);
    }

    @Override
    public Expression visitNonAggregateFunctionCall(final RelationalParser.NonAggregateFunctionCallContext ctx) {
        return expressionVisitor.visitNonAggregateFunctionCall(ctx);
    }

    @Override
    public Expression visitNonAggregateWindowedFunction(RelationalParser.NonAggregateWindowedFunctionContext ctx) {
        return expressionVisitor.visitNonAggregateWindowedFunction(ctx);
    }

    @Override
    public WindowSpecExpression visitOverClause(RelationalParser.OverClauseContext ctx) {
        return expressionVisitor.visitOverClause(ctx);
    }

    @Override
    public Expressions visitPartitionClause(final RelationalParser.PartitionClauseContext ctx) {
        return expressionVisitor.visitPartitionClause(ctx);
    }

    @Override
    public Object visitWindowName(RelationalParser.WindowNameContext ctx) {
        return visitChildren(ctx);
    }


    @Override
    public Expressions visitWindowOptionsClause(final RelationalParser.WindowOptionsClauseContext ctx) {
        return expressionVisitor.visitWindowOptionsClause(ctx);
    }

    @Override
    public Expression visitWindowOption(final RelationalParser.WindowOptionContext ctx) {
        return expressionVisitor.visitWindowOption(ctx);
    }

    @Override
    public Object visitScalarFunctionName(RelationalParser.ScalarFunctionNameContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expressions visitFunctionArgs(RelationalParser.FunctionArgsContext ctx) {
        return expressionVisitor.visitFunctionArgs(ctx);
    }

    @Override
    public Object visitFunctionArg(RelationalParser.FunctionArgContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitNamedFunctionArg(final RelationalParser.NamedFunctionArgContext ctx) {
        return expressionVisitor.visitNamedFunctionArg(ctx);
    }

    @Override
    public Expression visitNotExpression(RelationalParser.NotExpressionContext ctx) {
        return expressionVisitor.visitNotExpression(ctx);
    }

    @Override
    public Expression visitLogicalExpression(RelationalParser.LogicalExpressionContext ctx) {
        return expressionVisitor.visitLogicalExpression(ctx);
    }

    @Override
    public Expression visitPredicatedExpression(RelationalParser.PredicatedExpressionContext ctx) {
        return expressionVisitor.visitPredicatedExpression(ctx);
    }

    @Override
    public Expression visitBinaryComparisonPredicate(RelationalParser.BinaryComparisonPredicateContext ctx) {
        return expressionVisitor.visitBinaryComparisonPredicate(ctx);
    }

    @Override
    public Expression visitSubscriptExpression(RelationalParser.SubscriptExpressionContext ctx) {
        return expressionVisitor.visitSubscriptExpression(ctx);
    }

    @Override
    public Expression visitInList(RelationalParser.InListContext ctx) {
        return expressionVisitor.visitInList(ctx);
    }

    @Override
    public Object visitConstantExpressionAtom(RelationalParser.ConstantExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitFunctionCallExpressionAtom(RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return expressionVisitor.visitFunctionCallExpressionAtom(ctx);
    }

    @Override
    public Object visitFullColumnNameExpressionAtom(RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitBitExpressionAtom(RelationalParser.BitExpressionAtomContext ctx) {
        return expressionVisitor.visitBitExpressionAtom(ctx);
    }

    @Override
    public Expression visitPreparedStatementParameterAtom(RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return expressionVisitor.visitPreparedStatementParameterAtom(ctx);
    }

    @Override
    public Object visitRecordConstructorExpressionAtom(RelationalParser.RecordConstructorExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitArrayConstructorExpressionAtom(RelationalParser.ArrayConstructorExpressionAtomContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitMathExpressionAtom(RelationalParser.MathExpressionAtomContext ctx) {
        return expressionVisitor.visitMathExpressionAtom(ctx);
    }

    @Override
    public Expression visitExistsExpressionAtom(RelationalParser.ExistsExpressionAtomContext ctx) {
        return expressionVisitor.visitExistsExpressionAtom(ctx);
    }

    @Override
    public Expression visitPreparedStatementParameter(RelationalParser.PreparedStatementParameterContext ctx) {
        return expressionVisitor.visitPreparedStatementParameter(ctx);
    }

    @Override
    public Object visitUnaryOperator(RelationalParser.UnaryOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitComparisonOperator(RelationalParser.ComparisonOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitLogicalOperator(RelationalParser.LogicalOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitBitOperator(RelationalParser.BitOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitMathOperator(RelationalParser.MathOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitJsonOperator(RelationalParser.JsonOperatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitCharsetNameBase(RelationalParser.CharsetNameBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitIntervalTypeBase(RelationalParser.IntervalTypeBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitKeywordsCanBeId(RelationalParser.KeywordsCanBeIdContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitFunctionNameBase(RelationalParser.FunctionNameBaseContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitFunctionNameKeyword(RelationalParser.FunctionNameKeywordContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitExecuteContinuationStatement(RelationalParser.ExecuteContinuationStatementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public QueryPlan visitCopyExportStatement(RelationalParser.CopyExportStatementContext ctx) {
        return metadataPlanVisitor.visitCopyExportStatement(ctx);
    }

    @Override
    public QueryPlan visitCopyImportStatement(RelationalParser.CopyImportStatementContext ctx) {
        return metadataPlanVisitor.visitCopyImportStatement(ctx);
    }

    public DdlQueryFactory getDdlQueryFactory() {
        return ddlQueryFactory;
    }

    public URI getDbUri() {
        return dbUri;
    }
}
