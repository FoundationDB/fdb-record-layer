/*
 * DelegatingVisitor.java
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
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.WindowSpecExpression;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Set;

@API(API.Status.EXPERIMENTAL)
public class DelegatingVisitor<D extends TypedVisitor> implements TypedVisitor {

    private final D delegate;

    public DelegatingVisitor(D delegate) {
        this.delegate = delegate;
    }

    public D getDelegate() {
        return delegate;
    }

    @Override
    public Object visitRoot(RelationalParser.RootContext ctx) {
        return getDelegate().visitRoot(ctx);
    }

    @Override
    public Object visitStatements(RelationalParser.StatementsContext ctx) {
        return getDelegate().visitStatements(ctx);
    }

    @Nullable
    @Override
    public Object visitStatement(RelationalParser.StatementContext ctx) {
        return getDelegate().visitStatement(ctx);
    }

    @Override
    public QueryPlan.LogicalQueryPlan visitDmlStatement(RelationalParser.DmlStatementContext ctx) {
        return getDelegate().visitDmlStatement(ctx);
    }

    @Override
    public Object visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        return getDelegate().visitDdlStatement(ctx);
    }

    @Override
    public Object visitTransactionStatement(RelationalParser.TransactionStatementContext ctx) {
        return getDelegate().visitTransactionStatement(ctx);
    }

    @Override
    public Object visitPreparedStatement(RelationalParser.PreparedStatementContext ctx) {
        return getDelegate().visitPreparedStatement(ctx);
    }

    @Override
    public Object visitAdministrationStatement(RelationalParser.AdministrationStatementContext ctx) {
        return getDelegate().visitAdministrationStatement(ctx);
    }

    @Override
    public Object visitUtilityStatement(RelationalParser.UtilityStatementContext ctx) {
        return getDelegate().visitUtilityStatement(ctx);
    }

    @Override
    public Object visitTemplateClause(RelationalParser.TemplateClauseContext ctx) {
        return getDelegate().visitTemplateClause(ctx);
    }

    @Override
    public ProceduralPlan visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx) {
        return getDelegate().visitCreateSchemaStatement(ctx);
    }

    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        return getDelegate().visitCreateSchemaTemplateStatement(ctx);
    }

    @Override
    public ProceduralPlan visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx) {
        return getDelegate().visitCreateDatabaseStatement(ctx);
    }

    @Override
    public Object visitOptionsClause(RelationalParser.OptionsClauseContext ctx) {
        return getDelegate().visitOptionsClause(ctx);
    }

    @Override
    public Object visitOption(RelationalParser.OptionContext ctx) {
        return getDelegate().visitOption(ctx);
    }

    @Override
    public ProceduralPlan visitDropDatabaseStatement(RelationalParser.DropDatabaseStatementContext ctx) {
        return getDelegate().visitDropDatabaseStatement(ctx);
    }

    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return getDelegate().visitDropSchemaTemplateStatement(ctx);
    }

    @Override
    public ProceduralPlan visitDropSchemaStatement(RelationalParser.DropSchemaStatementContext ctx) {
        return getDelegate().visitDropSchemaStatement(ctx);
    }

    @Override
    public RecordLayerTable visitStructDefinition(RelationalParser.StructDefinitionContext ctx) {
        return getDelegate().visitStructDefinition(ctx);
    }

    @Override
    public RecordLayerTable visitTableDefinition(RelationalParser.TableDefinitionContext ctx) {
        return getDelegate().visitTableDefinition(ctx);
    }

    @Override
    public Object visitColumnDefinition(RelationalParser.ColumnDefinitionContext ctx) {
        return getDelegate().visitColumnDefinition(ctx);
    }

    @Override
    public DataType visitFunctionColumnType(final RelationalParser.FunctionColumnTypeContext ctx) {
        return getDelegate().visitFunctionColumnType(ctx);
    }

    @Override
    public Object visitColumnType(RelationalParser.ColumnTypeContext ctx) {
        return getDelegate().visitColumnType(ctx);
    }

    @Override
    public Object visitPrimitiveType(RelationalParser.PrimitiveTypeContext ctx) {
        return getDelegate().visitPrimitiveType(ctx);
    }

    @Override
    public Object visitVectorType(final RelationalParser.VectorTypeContext ctx) {
        return getDelegate().visitVectorType(ctx);
    }

    @Override
    public Object visitVectorElementType(final RelationalParser.VectorElementTypeContext ctx) {
        return getDelegate().visitVectorElementType(ctx);
    }

    @Override
    public Boolean visitNullColumnConstraint(RelationalParser.NullColumnConstraintContext ctx) {
        return getDelegate().visitNullColumnConstraint(ctx);
    }

    @Override
    public Object visitPrimaryKeyDefinition(RelationalParser.PrimaryKeyDefinitionContext ctx) {
        return getDelegate().visitPrimaryKeyDefinition(ctx);
    }

    @Override
    public List<Identifier> visitFullIdList(RelationalParser.FullIdListContext ctx) {
        return getDelegate().visitFullIdList(ctx);
    }

    @Override
    public DataType.Named visitEnumDefinition(RelationalParser.EnumDefinitionContext ctx) {
        return getDelegate().visitEnumDefinition(ctx);
    }

    @Override
    public Object visitIndexType(final RelationalParser.IndexTypeContext ctx) {
        return getDelegate().visitIndexType(ctx);
    }

    @Override
    public Object visitIndexPartitionClause(final RelationalParser.IndexPartitionClauseContext ctx) {
        return getDelegate().visitIndexPartitionClause(ctx);
    }

    @Override
    public Object visitIndexOptions(final RelationalParser.IndexOptionsContext ctx) {
        return getDelegate().visitIndexOptions(ctx);
    }

    @Override
    public Object visitIndexOption(final RelationalParser.IndexOptionContext ctx) {
        return getDelegate().visitIndexOption(ctx);
    }

    @Override
    public Object visitVectorIndexOptions(final RelationalParser.VectorIndexOptionsContext ctx) {
        return getDelegate().visitVectorIndexOptions(ctx);
    }

    @Override
    public Object visitVectorIndexOption(final RelationalParser.VectorIndexOptionContext ctx) {
        return getDelegate().visitVectorIndexOption(ctx);
    }

    @Override
    public Object visitHnswMetric(final RelationalParser.HnswMetricContext ctx) {
        return getDelegate().visitHnswMetric(ctx);
    }

    @Override
    public Object visitVectorEngine(final RelationalParser.VectorEngineContext ctx) {
        return getDelegate().visitVectorEngine(ctx);
    }

    @Override
    public Object visitVectorIndexOptionValue(final RelationalParser.VectorIndexOptionValueContext ctx) {
        return getDelegate().visitVectorIndexOptionValue(ctx);
    }

    @Override
    public Object visitIndexAttributes(RelationalParser.IndexAttributesContext ctx) {
        return getDelegate().visitIndexAttributes(ctx);
    }

    @Override
    public Object visitIndexAttribute(RelationalParser.IndexAttributeContext ctx) {
        return getDelegate().visitIndexAttribute(ctx);
    }

    @Override
    public ProceduralPlan visitCreateTempFunction(final RelationalParser.CreateTempFunctionContext ctx) {
        return getDelegate().visitCreateTempFunction(ctx);
    }

    @Override
    public ProceduralPlan visitDropTempFunction(final RelationalParser.DropTempFunctionContext ctx) {
        return getDelegate().visitDropTempFunction(ctx);
    }

    @Override
    public Object visitViewDefinition(final RelationalParser.ViewDefinitionContext ctx) {
        return getDelegate().visitViewDefinition(ctx);
    }

    @Override
    public Object visitStoredQueryDefinition(final RelationalParser.StoredQueryDefinitionContext ctx) {
        return getDelegate().visitStoredQueryDefinition(ctx);
    }

    @Override
    public Object visitDeclareBlock(final RelationalParser.DeclareBlockContext ctx) {
        return getDelegate().visitDeclareBlock(ctx);
    }

    @Override
    public Object visitDeclaredFunction(final RelationalParser.DeclaredFunctionContext ctx) {
        return getDelegate().visitDeclaredFunction(ctx);
    }

    @Override
    public CompiledSqlFunction visitTempSqlInvokedFunction(final RelationalParser.TempSqlInvokedFunctionContext ctx) {
        return getDelegate().visitTempSqlInvokedFunction(ctx);
    }

    @Override
    public UserDefinedFunction visitSqlInvokedFunction(RelationalParser.SqlInvokedFunctionContext ctx) {
        return getDelegate().visitSqlInvokedFunction(ctx);
    }

    @Override
    public Expressions visitSqlParameterDeclarationList(final RelationalParser.SqlParameterDeclarationListContext ctx) {
        return getDelegate().visitSqlParameterDeclarationList(ctx);
    }

    @Override
    public Expressions visitSqlParameterDeclarations(final RelationalParser.SqlParameterDeclarationsContext ctx) {
        return getDelegate().visitSqlParameterDeclarations(ctx);
    }

    @Override
    public Expression visitSqlParameterDeclaration(final RelationalParser.SqlParameterDeclarationContext ctx) {
        return getDelegate().visitSqlParameterDeclaration(ctx);
    }

    @Override
    public Object visitParameterMode(final RelationalParser.ParameterModeContext ctx) {
        return getDelegate().visitParameterMode(ctx);
    }

    @Override
    public Object visitReturnsClause(final RelationalParser.ReturnsClauseContext ctx) {
        return getDelegate().visitReturnsClause(ctx);
    }

    @Override
    public DataType visitReturnsType(final RelationalParser.ReturnsTypeContext ctx) {
        return getDelegate().visitReturnsType(ctx);
    }

    @Override
    public Object visitReturnsTableType(final RelationalParser.ReturnsTableTypeContext ctx) {
        return getDelegate().visitReturnsTableType(ctx);
    }

    @Override
    public Object visitTableFunctionColumnList(final RelationalParser.TableFunctionColumnListContext ctx) {
        return getDelegate().visitTableFunctionColumnList(ctx);
    }

    @Override
    public Object visitTableFunctionColumnListElement(final RelationalParser.TableFunctionColumnListElementContext ctx) {
        return getDelegate().visitTableFunctionColumnListElement(ctx);
    }

    @Override
    public Object visitRoutineCharacteristics(final RelationalParser.RoutineCharacteristicsContext ctx) {
        return getDelegate().visitRoutineCharacteristics(ctx);
    }

    @Override
    public Object visitLanguageClause(final RelationalParser.LanguageClauseContext ctx) {
        return getDelegate().visitLanguageClause(ctx);
    }

    @Override
    public Object visitLanguageName(final RelationalParser.LanguageNameContext ctx) {
        return getDelegate().visitLanguageName(ctx);
    }

    @Override
    public Object visitParameterStyle(final RelationalParser.ParameterStyleContext ctx) {
        return getDelegate().visitParameterStyle(ctx);
    }

    @Override
    public Object visitDeterministicCharacteristic(final RelationalParser.DeterministicCharacteristicContext ctx) {
        return getDelegate().visitDeterministicCharacteristic(ctx);
    }

    @Override
    public Object visitNullCallClause(final RelationalParser.NullCallClauseContext ctx) {
        return getDelegate().visitNullCallClause(ctx);
    }

    @Override
    public Object visitDispatchClause(final RelationalParser.DispatchClauseContext ctx) {
        return getDelegate().visitDispatchClause(ctx);
    }

    @Override
    public LogicalOperator visitStatementBody(final RelationalParser.StatementBodyContext ctx) {
        return getDelegate().visitStatementBody(ctx);
    }

    @Override
    public RecordLayerInvokedRoutine visitFunctionSpecification(final RelationalParser.FunctionSpecificationContext ctx) {
        return getDelegate().visitFunctionSpecification(ctx);
    }

    @Override
    public Object visitCharSet(RelationalParser.CharSetContext ctx) {
        return getDelegate().visitCharSet(ctx);
    }

    @Override
    public Object visitIntervalType(RelationalParser.IntervalTypeContext ctx) {
        return getDelegate().visitIntervalType(ctx);
    }

    @Override
    public Object visitSchemaId(RelationalParser.SchemaIdContext ctx) {
        return getDelegate().visitSchemaId(ctx);
    }

    @Override
    public Object visitPath(RelationalParser.PathContext ctx) {
        return getDelegate().visitPath(ctx);
    }

    @Override
    public Object visitSchemaTemplateId(RelationalParser.SchemaTemplateIdContext ctx) {
        return getDelegate().visitSchemaTemplateId(ctx);
    }

    @Override
    public LogicalOperator visitDeleteStatement(RelationalParser.DeleteStatementContext ctx) {
        return getDelegate().visitDeleteStatement(ctx);
    }

    @Override
    public LogicalOperator visitInsertStatement(RelationalParser.InsertStatementContext ctx) {
        return getDelegate().visitInsertStatement(ctx);
    }

    @Override
    public QueryPlan.LogicalQueryPlan visitSelectStatement(RelationalParser.SelectStatementContext ctx) {
        return getDelegate().visitSelectStatement(ctx);
    }

    @Override
    public LogicalOperator visitQuery(RelationalParser.QueryContext ctx) {
        return getDelegate().visitQuery(ctx);
    }

    @Nullable
    @Override
    public Void visitCtes(RelationalParser.CtesContext ctx) {
        return getDelegate().visitCtes(ctx);
    }

    @Override
    public Object visitTraversalOrderClause(final RelationalParser.TraversalOrderClauseContext ctx) {
        return getDelegate().visitTraversalOrderClause(ctx);
    }

    @Override
    public LogicalOperator visitNamedQuery(RelationalParser.NamedQueryContext ctx) {
        return getDelegate().visitNamedQuery(ctx);
    }

    @Override
    public LogicalOperator visitTableFunction(RelationalParser.TableFunctionContext ctx) {
        return getDelegate().visitTableFunction(ctx);
    }

    @Override
    public Expressions visitNamedOrUnnamedFunctionArgs(RelationalParser.NamedOrUnnamedFunctionArgsContext ctx) {
        return getDelegate().visitNamedOrUnnamedFunctionArgs(ctx);
    }

    @Override
    public Identifier visitTableFunctionName(RelationalParser.TableFunctionNameContext ctx) {
        return getDelegate().visitTableFunctionName(ctx);
    }

    @Override
    public Expression visitContinuationAtom(RelationalParser.ContinuationAtomContext ctx) {
        return getDelegate().visitContinuationAtom(ctx);
    }

    @Override
    public LogicalOperator visitQueryTermDefault(RelationalParser.QueryTermDefaultContext ctx) {
        return getDelegate().visitQueryTermDefault(ctx);
    }

    @Override
    public LogicalOperator visitSetQuery(RelationalParser.SetQueryContext ctx) {
        return getDelegate().visitSetQuery(ctx);
    }

    @Override
    public LogicalOperator visitInsertStatementValueSelect(RelationalParser.InsertStatementValueSelectContext ctx) {
        return getDelegate().visitInsertStatementValueSelect(ctx);
    }

    @Override
    public LogicalOperator visitInsertStatementValueValues(RelationalParser.InsertStatementValueValuesContext ctx) {
        return getDelegate().visitInsertStatementValueValues(ctx);
    }

    @Override
    public Expressions visitUpdatedElement(RelationalParser.UpdatedElementContext ctx) {
        return getDelegate().visitUpdatedElement(ctx);
    }

    @Override
    public Object visitAssignmentField(RelationalParser.AssignmentFieldContext ctx) {
        return getDelegate().visitAssignmentField(ctx);
    }

    @Override
    public LogicalOperator visitUpdateStatement(RelationalParser.UpdateStatementContext ctx) {
        return getDelegate().visitUpdateStatement(ctx);
    }

    @Override
    public List<OrderByExpression> visitOrderByClause(RelationalParser.OrderByClauseContext ctx) {
        return getDelegate().visitOrderByClause(ctx);
    }

    @Override
    public OrderByExpression visitOrderByExpression(RelationalParser.OrderByExpressionContext ctx) {
        return getDelegate().visitOrderByExpression(ctx);
    }

    @Override
    public RecordLayerIndex visitIndexAsSelectDefinition(RelationalParser.IndexAsSelectDefinitionContext ctx) {
        return getDelegate().visitIndexAsSelectDefinition(ctx);
    }

    @Override
    public RecordLayerIndex visitIndexOnSourceDefinition(RelationalParser.IndexOnSourceDefinitionContext ctx) {
        return getDelegate().visitIndexOnSourceDefinition(ctx);
    }

    @Override
    public RecordLayerIndex visitVectorIndexDefinition(final RelationalParser.VectorIndexDefinitionContext ctx) {
        return getDelegate().visitVectorIndexDefinition(ctx);
    }

    @Override
    public Object visitIndexColumnList(RelationalParser.IndexColumnListContext ctx) {
        return getDelegate().visitIndexColumnList(ctx);
    }

    @Override
    public Object visitIndexColumnSpec(RelationalParser.IndexColumnSpecContext ctx) {
        return getDelegate().visitIndexColumnSpec(ctx);
    }

    @Override
    public Object visitIncludeClause(RelationalParser.IncludeClauseContext ctx) {
        return getDelegate().visitIncludeClause(ctx);
    }

    @Override
    @Nullable
    public Void visitTableSources(RelationalParser.TableSourcesContext ctx) {
        return getDelegate().visitTableSources(ctx);
    }

    @Nullable
    @Override
    public Void visitTableSourceBase(RelationalParser.TableSourceBaseContext ctx) {
        return getDelegate().visitTableSourceBase(ctx);
    }

    @Override
    public LogicalOperator visitAtomTableItem(RelationalParser.AtomTableItemContext ctx) {
        return getDelegate().visitAtomTableItem(ctx);
    }

    @Override
    public LogicalOperator visitSubqueryTableItem(RelationalParser.SubqueryTableItemContext ctx) {
        return getDelegate().visitSubqueryTableItem(ctx);
    }

    @Override
    public LogicalOperator visitInlineTableItem(final RelationalParser.InlineTableItemContext ctx) {
        return getDelegate().visitInlineTableItem(ctx);
    }

    @Override
    public LogicalOperator visitTableValuedFunction(final RelationalParser.TableValuedFunctionContext ctx) {
        return getDelegate().visitTableValuedFunction(ctx);
    }

    @Override
    public Set<String> visitIndexHint(RelationalParser.IndexHintContext ctx) {
        return getDelegate().visitIndexHint(ctx);
    }

    @Override
    public Object visitIndexHintType(RelationalParser.IndexHintTypeContext ctx) {
        return getDelegate().visitIndexHintType(ctx);
    }

    @Override
    public NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> visitInlineTableDefinition(RelationalParser.InlineTableDefinitionContext ctx) {
        return getDelegate().visitInlineTableDefinition(ctx);
    }


    @Nullable
    @Override
    public Object visitInnerJoin(RelationalParser.InnerJoinContext ctx) {
        return getDelegate().visitInnerJoin(ctx);
    }

    @Override
    public Object visitStraightJoin(RelationalParser.StraightJoinContext ctx) {
        return getDelegate().visitStraightJoin(ctx);
    }

    @Nullable
    @Override
    public Object visitOuterJoin(RelationalParser.OuterJoinContext ctx) {
        return getDelegate().visitOuterJoin(ctx);
    }

    @Override
    public Object visitNaturalJoin(RelationalParser.NaturalJoinContext ctx) {
        return getDelegate().visitNaturalJoin(ctx);
    }

    @Override
    public LogicalOperator visitSimpleTable(RelationalParser.SimpleTableContext ctx) {
        return getDelegate().visitSimpleTable(ctx);
    }

    @Override
    public LogicalOperator visitParenthesisQuery(RelationalParser.ParenthesisQueryContext ctx) {
        return getDelegate().visitParenthesisQuery(ctx);
    }

    @Override
    public Expressions visitSelectElements(RelationalParser.SelectElementsContext ctx) {
        return getDelegate().visitSelectElements(ctx);
    }

    @Override
    public Expression visitSelectStarElement(RelationalParser.SelectStarElementContext ctx) {
        return getDelegate().visitSelectStarElement(ctx);
    }

    @Override
    public Object visitSelectQualifierStarElement(RelationalParser.SelectQualifierStarElementContext ctx) {
        return getDelegate().visitSelectQualifierStarElement(ctx);
    }

    @Override
    public Expression visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext ctx) {
        return getDelegate().visitSelectExpressionElement(ctx);
    }

    @Override
    @Nullable
    public Void visitFromClause(RelationalParser.FromClauseContext ctx) {
        return getDelegate().visitFromClause(ctx);
    }

    @Override
    public Expressions visitGroupByClause(RelationalParser.GroupByClauseContext ctx) {
        return getDelegate().visitGroupByClause(ctx);
    }

    @Override
    public Expression visitWhereExpr(RelationalParser.WhereExprContext ctx) {
        return getDelegate().visitWhereExpr(ctx);
    }

    @Override
    public Expression visitHavingClause(RelationalParser.HavingClauseContext ctx) {
        return getDelegate().visitHavingClause(ctx);
    }

    @Override
    public Expression visitQualifyClause(final RelationalParser.QualifyClauseContext ctx) {
        return getDelegate().visitQualifyClause(ctx);
    }

    @Override
    public Expression visitGroupByItem(RelationalParser.GroupByItemContext ctx) {
        return getDelegate().visitGroupByItem(ctx);
    }

    @Override
    public Expression visitLimitClause(RelationalParser.LimitClauseContext ctx) {
        return getDelegate().visitLimitClause(ctx);
    }

    @Override
    public Expression visitLimitClauseAtom(RelationalParser.LimitClauseAtomContext ctx) {
        return getDelegate().visitLimitClauseAtom(ctx);
    }

    @Override
    public Object visitStatementOptions(RelationalParser.StatementOptionsContext ctx) {
        return getDelegate().visitStatementOptions(ctx);
    }

    @Override
    public Object visitStatementOption(RelationalParser.StatementOptionContext ctx) {
        return getDelegate().visitStatementOption(ctx);
    }

    @Override
    public Object visitStartTransaction(RelationalParser.StartTransactionContext ctx) {
        return getDelegate().visitStartTransaction(ctx);
    }

    @Override
    public Object visitCommitStatement(RelationalParser.CommitStatementContext ctx) {
        return getDelegate().visitCommitStatement(ctx);
    }

    @Override
    public Object visitRollbackStatement(RelationalParser.RollbackStatementContext ctx) {
        return getDelegate().visitRollbackStatement(ctx);
    }

    @Override
    public Object visitSetAutocommitStatement(RelationalParser.SetAutocommitStatementContext ctx) {
        return getDelegate().visitSetAutocommitStatement(ctx);
    }

    @Override
    public Object visitSetTransactionStatement(RelationalParser.SetTransactionStatementContext ctx) {
        return getDelegate().visitSetTransactionStatement(ctx);
    }

    @Override
    public Object visitTransactionOption(RelationalParser.TransactionOptionContext ctx) {
        return getDelegate().visitTransactionOption(ctx);
    }

    @Override
    public Object visitTransactionLevel(RelationalParser.TransactionLevelContext ctx) {
        return getDelegate().visitTransactionLevel(ctx);
    }

    @Override
    public Object visitPrepareStatement(RelationalParser.PrepareStatementContext ctx) {
        return getDelegate().visitPrepareStatement(ctx);
    }

    @Override
    public Object visitExecuteStatement(RelationalParser.ExecuteStatementContext ctx) {
        return getDelegate().visitExecuteStatement(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(RelationalParser.ShowDatabasesStatementContext ctx) {
        return getDelegate().visitShowDatabasesStatement(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        return getDelegate().visitShowSchemaTemplatesStatement(ctx);
    }

    @Override
    public Object visitSetVariable(RelationalParser.SetVariableContext ctx) {
        return getDelegate().visitSetVariable(ctx);
    }

    @Override
    public Object visitSetCharset(RelationalParser.SetCharsetContext ctx) {
        return getDelegate().visitSetCharset(ctx);
    }

    @Override
    public Object visitSetNames(RelationalParser.SetNamesContext ctx) {
        return getDelegate().visitSetNames(ctx);
    }

    @Override
    public Object visitSetTransaction(RelationalParser.SetTransactionContext ctx) {
        return getDelegate().visitSetTransaction(ctx);
    }

    @Override
    public Object visitSetAutocommit(RelationalParser.SetAutocommitContext ctx) {
        return getDelegate().visitSetAutocommit(ctx);
    }

    @Override
    public Object visitSetNewValueInsideTrigger(RelationalParser.SetNewValueInsideTriggerContext ctx) {
        return getDelegate().visitSetNewValueInsideTrigger(ctx);
    }

    @Override
    public Object visitVariableClause(RelationalParser.VariableClauseContext ctx) {
        return getDelegate().visitVariableClause(ctx);
    }

    @Override
    public Object visitKillStatement(RelationalParser.KillStatementContext ctx) {
        return getDelegate().visitKillStatement(ctx);
    }

    @Override
    public Object visitResetStatement(RelationalParser.ResetStatementContext ctx) {
        return getDelegate().visitResetStatement(ctx);
    }

    @Override
    public Object visitExecuteContinuationStatement(RelationalParser.ExecuteContinuationStatementContext ctx) {
        return getDelegate().visitExecuteContinuationStatement(ctx);
    }

    @Override
    public QueryPlan visitCopyExportStatement(RelationalParser.CopyExportStatementContext ctx) {
        return (QueryPlan) getDelegate().visitCopyExportStatement(ctx);
    }

    @Override
    public QueryPlan visitCopyImportStatement(RelationalParser.CopyImportStatementContext ctx) {
        return (QueryPlan) getDelegate().visitCopyImportStatement(ctx);
    }

    @Override
    public Object visitIncarnationOption(RelationalParser.IncarnationOptionContext ctx) {
        return getDelegate().visitIncarnationOption(ctx);
    }

    @Override
    public Object visitTableIndexes(RelationalParser.TableIndexesContext ctx) {
        return getDelegate().visitTableIndexes(ctx);
    }

    @Override
    public Object visitLoadedTableIndexes(RelationalParser.LoadedTableIndexesContext ctx) {
        return getDelegate().visitLoadedTableIndexes(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        return getDelegate().visitSimpleDescribeSchemaStatement(ctx);
    }

    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        return getDelegate().visitSimpleDescribeSchemaTemplateStatement(ctx);
    }

    @Override
    public QueryPlan.LogicalQueryPlan visitFullDescribeStatement(RelationalParser.FullDescribeStatementContext ctx) {
        return getDelegate().visitFullDescribeStatement(ctx);
    }

    @Override
    public Object visitHelpStatement(RelationalParser.HelpStatementContext ctx) {
        return getDelegate().visitHelpStatement(ctx);
    }

    @Override
    public Object visitDescribeStatements(RelationalParser.DescribeStatementsContext ctx) {
        return getDelegate().visitDescribeStatements(ctx);
    }

    @Override
    public Object visitDescribeConnection(RelationalParser.DescribeConnectionContext ctx) {
        return getDelegate().visitDescribeConnection(ctx);
    }

    @Override
    public Identifier visitFullId(RelationalParser.FullIdContext ctx) {
        return getDelegate().visitFullId(ctx);
    }

    @Override
    public Expression visitUserDefinedMacroFunctionStatementBody(RelationalParser.UserDefinedMacroFunctionStatementBodyContext ctx) {
        return getDelegate().visitUserDefinedMacroFunctionStatementBody(ctx);
    }

    @Override
    public Identifier visitTableName(RelationalParser.TableNameContext ctx) {
        return getDelegate().visitTableName(ctx);
    }

    @Override
    public Expression visitFullColumnName(RelationalParser.FullColumnNameContext ctx) {
        return getDelegate().visitFullColumnName(ctx);
    }

    @Override
    public Identifier visitIndexColumnName(RelationalParser.IndexColumnNameContext ctx) {
        return getDelegate().visitIndexColumnName(ctx);
    }

    @Override
    public Identifier visitCharsetName(RelationalParser.CharsetNameContext ctx) {
        return getDelegate().visitCharsetName(ctx);
    }

    @Override
    public Identifier visitCollationName(RelationalParser.CollationNameContext ctx) {
        return getDelegate().visitCollationName(ctx);
    }

    @Override
    public Identifier visitUid(RelationalParser.UidContext ctx) {
        return getDelegate().visitUid(ctx);
    }

    @Override
    public Identifier visitSimpleId(RelationalParser.SimpleIdContext ctx) {
        return getDelegate().visitSimpleId(ctx);
    }

    @Override
    public Object visitNullNotnull(RelationalParser.NullNotnullContext ctx) {
        return getDelegate().visitNullNotnull(ctx);
    }

    @Override
    public Expression visitDecimalLiteral(RelationalParser.DecimalLiteralContext ctx) {
        return getDelegate().visitDecimalLiteral(ctx);
    }

    @Override
    public Expression visitStringLiteral(RelationalParser.StringLiteralContext ctx) {
        return getDelegate().visitStringLiteral(ctx);
    }

    @Override
    public Expression visitBooleanLiteral(RelationalParser.BooleanLiteralContext ctx) {
        return getDelegate().visitBooleanLiteral(ctx);
    }

    @Override
    public Expression visitBytesLiteral(RelationalParser.BytesLiteralContext ctx) {
        return getDelegate().visitBytesLiteral(ctx);
    }

    @Override
    public Expression visitNullLiteral(RelationalParser.NullLiteralContext ctx) {
        return getDelegate().visitNullLiteral(ctx);
    }

    @Override
    public Expression visitStringConstant(RelationalParser.StringConstantContext ctx) {
        return getDelegate().visitStringConstant(ctx);
    }

    @Override
    public Expression visitDecimalConstant(RelationalParser.DecimalConstantContext ctx) {
        return getDelegate().visitDecimalConstant(ctx);
    }

    @Override
    public Expression visitNegativeDecimalConstant(RelationalParser.NegativeDecimalConstantContext ctx) {
        return getDelegate().visitNegativeDecimalConstant(ctx);
    }

    @Override
    public Expression visitBytesConstant(RelationalParser.BytesConstantContext ctx) {
        return getDelegate().visitBytesConstant(ctx);
    }

    @Override
    public Expression visitBooleanConstant(RelationalParser.BooleanConstantContext ctx) {
        return getDelegate().visitBooleanConstant(ctx);
    }

    @Override
    public Expression visitBitStringConstant(RelationalParser.BitStringConstantContext ctx) {
        return getDelegate().visitBitStringConstant(ctx);
    }

    @Override
    public Expression visitNullConstant(RelationalParser.NullConstantContext ctx) {
        return getDelegate().visitNullConstant(ctx);
    }

    @Override
    public Object visitStringDataType(RelationalParser.StringDataTypeContext ctx) {
        return getDelegate().visitStringDataType(ctx);
    }

    @Override
    public Object visitNationalStringDataType(RelationalParser.NationalStringDataTypeContext ctx) {
        return getDelegate().visitNationalStringDataType(ctx);
    }

    @Override
    public Object visitNationalVaryingStringDataType(RelationalParser.NationalVaryingStringDataTypeContext ctx) {
        return getDelegate().visitNationalVaryingStringDataType(ctx);
    }

    @Override
    public Object visitDimensionDataType(RelationalParser.DimensionDataTypeContext ctx) {
        return getDelegate().visitDimensionDataType(ctx);
    }

    @Override
    public Object visitSimpleDataType(RelationalParser.SimpleDataTypeContext ctx) {
        return getDelegate().visitSimpleDataType(ctx);
    }

    @Override
    public Object visitCollectionDataType(RelationalParser.CollectionDataTypeContext ctx) {
        return getDelegate().visitCollectionDataType(ctx);
    }

    @Override
    public Object visitSpatialDataType(RelationalParser.SpatialDataTypeContext ctx) {
        return getDelegate().visitSpatialDataType(ctx);
    }

    @Override
    public Object visitLongVarcharDataType(RelationalParser.LongVarcharDataTypeContext ctx) {
        return getDelegate().visitLongVarcharDataType(ctx);
    }

    @Override
    public Object visitLongVarbinaryDataType(RelationalParser.LongVarbinaryDataTypeContext ctx) {
        return getDelegate().visitLongVarbinaryDataType(ctx);
    }

    @Override
    public Object visitCollectionOptions(RelationalParser.CollectionOptionsContext ctx) {
        return getDelegate().visitCollectionOptions(ctx);
    }

    @Override
    public Object visitConvertedDataType(RelationalParser.ConvertedDataTypeContext ctx) {
        return getDelegate().visitConvertedDataType(ctx);
    }

    @Override
    public Object visitLengthOneDimension(RelationalParser.LengthOneDimensionContext ctx) {
        return getDelegate().visitLengthOneDimension(ctx);
    }

    @Override
    public Object visitLengthTwoDimension(RelationalParser.LengthTwoDimensionContext ctx) {
        return getDelegate().visitLengthTwoDimension(ctx);
    }

    @Override
    public Object visitLengthTwoOptionalDimension(RelationalParser.LengthTwoOptionalDimensionContext ctx) {
        return getDelegate().visitLengthTwoOptionalDimension(ctx);
    }

    @Override
    public List<Identifier> visitUidList(RelationalParser.UidListContext ctx) {
        return getDelegate().visitUidList(ctx);
    }

    @Override
    public Object visitUidWithNestings(RelationalParser.UidWithNestingsContext ctx) {
        return getDelegate().visitUidWithNestings(ctx);
    }

    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestingsInParens(RelationalParser.UidListWithNestingsInParensContext ctx) {
        return getDelegate().visitUidListWithNestingsInParens(ctx);
    }

    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestings(RelationalParser.UidListWithNestingsContext ctx) {
        return getDelegate().visitUidListWithNestings(ctx);
    }

    @Override
    public Object visitTables(RelationalParser.TablesContext ctx) {
        return getDelegate().visitTables(ctx);
    }

    @Override
    public Object visitIndexColumnNames(RelationalParser.IndexColumnNamesContext ctx) {
        return getDelegate().visitIndexColumnNames(ctx);
    }

    @Override
    public Expressions visitExpressions(RelationalParser.ExpressionsContext ctx) {
        return getDelegate().visitExpressions(ctx);
    }

    @Override
    public Object visitExpressionsWithDefaults(RelationalParser.ExpressionsWithDefaultsContext ctx) {
        return getDelegate().visitExpressionsWithDefaults(ctx);
    }

    @Override
    public Expression visitRecordConstructorForInsert(RelationalParser.RecordConstructorForInsertContext ctx) {
        return getDelegate().visitRecordConstructorForInsert(ctx);
    }

    @Override
    public Expression visitRecordConstructorForInlineTable(RelationalParser.RecordConstructorForInlineTableContext ctx) {
        return getDelegate().visitRecordConstructorForInlineTable(ctx);
    }

    @Override
    public Expression visitRecordConstructor(RelationalParser.RecordConstructorContext ctx) {
        return getDelegate().visitRecordConstructor(ctx);
    }

    @Override
    public Object visitOfTypeClause(RelationalParser.OfTypeClauseContext ctx) {
        return getDelegate().visitOfTypeClause(ctx);
    }

    @Override
    public Expression visitArrayConstructor(RelationalParser.ArrayConstructorContext ctx) {
        return getDelegate().visitArrayConstructor(ctx);
    }

    @Override
    public Object visitUserVariables(RelationalParser.UserVariablesContext ctx) {
        return getDelegate().visitUserVariables(ctx);
    }

    @Override
    public Object visitDefaultValue(RelationalParser.DefaultValueContext ctx) {
        return getDelegate().visitDefaultValue(ctx);
    }

    @Override
    public Object visitCurrentTimestamp(RelationalParser.CurrentTimestampContext ctx) {
        return getDelegate().visitCurrentTimestamp(ctx);
    }

    @Override
    public Object visitExpressionOrDefault(RelationalParser.ExpressionOrDefaultContext ctx) {
        return getDelegate().visitExpressionOrDefault(ctx);
    }

    @Override
    public Expression visitExpressionWithOptionalName(RelationalParser.ExpressionWithOptionalNameContext ctx) {
        return getDelegate().visitExpressionWithOptionalName(ctx);
    }

    @Nullable
    @Override
    public Object visitIfExists(RelationalParser.IfExistsContext ctx) {
        return getDelegate().visitIfExists(ctx);
    }

    @Nullable
    @Override
    public Object visitIfNotExists(RelationalParser.IfNotExistsContext ctx) {
        return getDelegate().visitIfNotExists(ctx);
    }

    @Override
    public Object visitUserDefinedScalarFunctionName(RelationalParser.UserDefinedScalarFunctionNameContext ctx) {
        return getDelegate().visitUserDefinedScalarFunctionName(ctx);
    }

    @Override
    public Expression visitUserDefinedScalarFunctionCall(RelationalParser.UserDefinedScalarFunctionCallContext ctx) {
        return getDelegate().visitUserDefinedScalarFunctionCall(ctx);
    }

    @Override
    public Expression visitAggregateFunctionCall(RelationalParser.AggregateFunctionCallContext ctx) {
        return getDelegate().visitAggregateFunctionCall(ctx);
    }

    @Override
    public Expression visitNonAggregateFunctionCall(RelationalParser.NonAggregateFunctionCallContext ctx) {
        return getDelegate().visitNonAggregateFunctionCall(ctx);
    }

    @Override
    public Object visitSpecificFunctionCall(RelationalParser.SpecificFunctionCallContext ctx) {
        return getDelegate().visitSpecificFunctionCall(ctx);
    }

    @Override
    public Expression visitScalarFunctionCall(RelationalParser.ScalarFunctionCallContext ctx) {
        return getDelegate().visitScalarFunctionCall(ctx);
    }

    @Override
    public Object visitSimpleFunctionCall(RelationalParser.SimpleFunctionCallContext ctx) {
        return getDelegate().visitSimpleFunctionCall(ctx);
    }

    @Override
    public Object visitDataTypeFunctionCall(RelationalParser.DataTypeFunctionCallContext ctx) {
        return getDelegate().visitDataTypeFunctionCall(ctx);
    }

    @Override
    public Object visitValuesFunctionCall(RelationalParser.ValuesFunctionCallContext ctx) {
        return getDelegate().visitValuesFunctionCall(ctx);
    }

    @Override
    public Object visitCaseExpressionFunctionCall(RelationalParser.CaseExpressionFunctionCallContext ctx) {
        return getDelegate().visitCaseExpressionFunctionCall(ctx);
    }

    @Override
    public Expression visitCaseFunctionCall(RelationalParser.CaseFunctionCallContext ctx) {
        return getDelegate().visitCaseFunctionCall(ctx);
    }

    @Override
    public Object visitCharFunctionCall(RelationalParser.CharFunctionCallContext ctx) {
        return getDelegate().visitCharFunctionCall(ctx);
    }

    @Override
    public Object visitPositionFunctionCall(RelationalParser.PositionFunctionCallContext ctx) {
        return getDelegate().visitPositionFunctionCall(ctx);
    }

    @Override
    public Object visitSubstrFunctionCall(RelationalParser.SubstrFunctionCallContext ctx) {
        return getDelegate().visitSubstrFunctionCall(ctx);
    }

    @Override
    public Object visitTrimFunctionCall(RelationalParser.TrimFunctionCallContext ctx) {
        return getDelegate().visitTrimFunctionCall(ctx);
    }

    @Override
    public Object visitWeightFunctionCall(RelationalParser.WeightFunctionCallContext ctx) {
        return getDelegate().visitWeightFunctionCall(ctx);
    }

    @Override
    public Object visitExtractFunctionCall(RelationalParser.ExtractFunctionCallContext ctx) {
        return getDelegate().visitExtractFunctionCall(ctx);
    }

    @Override
    public Object visitGetFormatFunctionCall(RelationalParser.GetFormatFunctionCallContext ctx) {
        return getDelegate().visitGetFormatFunctionCall(ctx);
    }

    @Override
    public Object visitCaseFuncAlternative(RelationalParser.CaseFuncAlternativeContext ctx) {
        return getDelegate().visitCaseFuncAlternative(ctx);
    }

    @Override
    public Object visitLevelWeightList(RelationalParser.LevelWeightListContext ctx) {
        return getDelegate().visitLevelWeightList(ctx);
    }

    @Override
    public Object visitLevelWeightRange(RelationalParser.LevelWeightRangeContext ctx) {
        return getDelegate().visitLevelWeightRange(ctx);
    }

    @Override
    public Object visitLevelInWeightListElement(RelationalParser.LevelInWeightListElementContext ctx) {
        return getDelegate().visitLevelInWeightListElement(ctx);
    }

    @Override
    public Expression visitAggregateWindowedFunction(RelationalParser.AggregateWindowedFunctionContext ctx) {
        return getDelegate().visitAggregateWindowedFunction(ctx);
    }

    @Override
    public Boolean visitNullTreatmentClause(RelationalParser.NullTreatmentClauseContext ctx) {
        return getDelegate().visitNullTreatmentClause(ctx);
    }

    @Override
    public Expression visitNonAggregateWindowedFunction(RelationalParser.NonAggregateWindowedFunctionContext ctx) {
        return getDelegate().visitNonAggregateWindowedFunction(ctx);
    }

    @Override
    public WindowSpecExpression visitOverClause(RelationalParser.OverClauseContext ctx) {
        return getDelegate().visitOverClause(ctx);
    }

    @Override
    public Object visitWindowName(RelationalParser.WindowNameContext ctx) {
        return getDelegate().visitWindowName(ctx);
    }

    @Override
    public Object visitWindowSpec(final RelationalParser.WindowSpecContext ctx) {
        return getDelegate().visitWindowSpec(ctx);
    }

    @Override
    public Expressions visitWindowOptionsClause(final RelationalParser.WindowOptionsClauseContext ctx) {
        return getDelegate().visitWindowOptionsClause(ctx);
    }

    @Override
    public Expression visitWindowOption(final RelationalParser.WindowOptionContext ctx) {
        return getDelegate().visitWindowOption(ctx);
    }

    @Override
    public Expressions visitPartitionClause(final RelationalParser.PartitionClauseContext ctx) {
        return getDelegate().visitPartitionClause(ctx);
    }

    @Override
    public Object visitScalarFunctionName(RelationalParser.ScalarFunctionNameContext ctx) {
        return getDelegate().visitScalarFunctionName(ctx);
    }

    @Override
    public Expressions visitFunctionArgs(RelationalParser.FunctionArgsContext ctx) {
        return getDelegate().visitFunctionArgs(ctx);
    }

    @Override
    public Object visitFunctionArg(RelationalParser.FunctionArgContext ctx) {
        return getDelegate().visitFunctionArg(ctx);
    }

    @Override
    public Expression visitNamedFunctionArg(final RelationalParser.NamedFunctionArgContext ctx) {
        return getDelegate().visitNamedFunctionArg(ctx);
    }

    @Override
    public Expression visitNotExpression(RelationalParser.NotExpressionContext ctx) {
        return getDelegate().visitNotExpression(ctx);
    }

    @Override
    public Expression visitLogicalExpression(RelationalParser.LogicalExpressionContext ctx) {
        return getDelegate().visitLogicalExpression(ctx);
    }

    @Override
    public Expression visitPredicatedExpression(RelationalParser.PredicatedExpressionContext ctx) {
        return getDelegate().visitPredicatedExpression(ctx);
    }

    @Override
    public Expression visitBinaryComparisonPredicate(RelationalParser.BinaryComparisonPredicateContext ctx) {
        return getDelegate().visitBinaryComparisonPredicate(ctx);
    }

    @Override
    public Expression visitSubscriptExpression(RelationalParser.SubscriptExpressionContext ctx) {
        return getDelegate().visitSubscriptExpression(ctx);
    }

    @Override
    public Expression visitInList(RelationalParser.InListContext ctx) {
        return getDelegate().visitInList(ctx);
    }

    @Override
    public Object visitConstantExpressionAtom(RelationalParser.ConstantExpressionAtomContext ctx) {
        return getDelegate().visitConstantExpressionAtom(ctx);
    }

    @Override
    public Expression visitFunctionCallExpressionAtom(RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return getDelegate().visitFunctionCallExpressionAtom(ctx);
    }

    @Override
    public Object visitFullColumnNameExpressionAtom(RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        return getDelegate().visitFullColumnNameExpressionAtom(ctx);
    }

    @Override
    public Expression visitBitExpressionAtom(RelationalParser.BitExpressionAtomContext ctx) {
        return getDelegate().visitBitExpressionAtom(ctx);
    }

    @Override
    public Expression visitPreparedStatementParameterAtom(RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return getDelegate().visitPreparedStatementParameterAtom(ctx);
    }

    @Override
    public Object visitRecordConstructorExpressionAtom(RelationalParser.RecordConstructorExpressionAtomContext ctx) {
        return getDelegate().visitRecordConstructorExpressionAtom(ctx);
    }

    @Override
    public Object visitArrayConstructorExpressionAtom(RelationalParser.ArrayConstructorExpressionAtomContext ctx) {
        return getDelegate().visitArrayConstructorExpressionAtom(ctx);
    }

    @Override
    public Expression visitMathExpressionAtom(RelationalParser.MathExpressionAtomContext ctx) {
        return getDelegate().visitMathExpressionAtom(ctx);
    }

    @Override
    public Expression visitExistsExpressionAtom(RelationalParser.ExistsExpressionAtomContext ctx) {
        return getDelegate().visitExistsExpressionAtom(ctx);
    }

    @Override
    public Object visitBetweenComparisonPredicate(final RelationalParser.BetweenComparisonPredicateContext ctx) {
        return getDelegate().visitBetweenComparisonPredicate(ctx);
    }

    @Override
    public Object visitInPredicate(final RelationalParser.InPredicateContext ctx) {
        return getDelegate().visitInPredicate(ctx);
    }

    @Override
    public Object visitLikePredicate(final RelationalParser.LikePredicateContext ctx) {
        return getDelegate().visitLikePredicate(ctx);
    }

    @Override
    public Object visitIsExpression(final RelationalParser.IsExpressionContext ctx) {
        return getDelegate().visitIsExpression(ctx);
    }

    @Override
    public Expression visitPreparedStatementParameter(RelationalParser.PreparedStatementParameterContext ctx) {
        return getDelegate().visitPreparedStatementParameter(ctx);
    }

    @Override
    public Object visitUnaryOperator(RelationalParser.UnaryOperatorContext ctx) {
        return getDelegate().visitUnaryOperator(ctx);
    }

    @Override
    public Object visitComparisonOperator(RelationalParser.ComparisonOperatorContext ctx) {
        return getDelegate().visitComparisonOperator(ctx);
    }

    @Override
    public Object visitLogicalOperator(RelationalParser.LogicalOperatorContext ctx) {
        return getDelegate().visitLogicalOperator(ctx);
    }

    @Override
    public Object visitBitOperator(RelationalParser.BitOperatorContext ctx) {
        return getDelegate().visitBitOperator(ctx);
    }

    @Override
    public Object visitMathOperator(RelationalParser.MathOperatorContext ctx) {
        return getDelegate().visitMathOperator(ctx);
    }

    @Override
    public Object visitJsonOperator(RelationalParser.JsonOperatorContext ctx) {
        return getDelegate().visitJsonOperator(ctx);
    }

    @Override
    public Object visitCharsetNameBase(RelationalParser.CharsetNameBaseContext ctx) {
        return getDelegate().visitCharsetNameBase(ctx);
    }

    @Override
    public Object visitIntervalTypeBase(RelationalParser.IntervalTypeBaseContext ctx) {
        return getDelegate().visitIntervalTypeBase(ctx);
    }

    @Override
    public Object visitKeywordsCanBeId(RelationalParser.KeywordsCanBeIdContext ctx) {
        return getDelegate().visitKeywordsCanBeId(ctx);
    }

    @Override
    public Object visitFunctionNameBase(RelationalParser.FunctionNameBaseContext ctx) {
        return getDelegate().visitFunctionNameBase(ctx);
    }

    @Override
    public Object visitFunctionNameKeyword(RelationalParser.FunctionNameKeywordContext ctx) {
        return getDelegate().visitFunctionNameKeyword(ctx);
    }

    @Override
    public Object visit(ParseTree tree) {
        return getDelegate().visit(tree);
    }

    @Override
    public Object visitChildren(RuleNode node) {
        return getDelegate().visitChildren(node);
    }

    @Override
    public Object visitOrderClause(RelationalParser.OrderClauseContext ctx) {
        return getDelegate().visitOrderClause(ctx);
    }

    @Override
    public Object visitTerminal(TerminalNode node) {
        return getDelegate().visitTerminal(node);
    }

    @Override
    public Object visitErrorNode(ErrorNode node) {
        return getDelegate().visitErrorNode(node);
    }
}
