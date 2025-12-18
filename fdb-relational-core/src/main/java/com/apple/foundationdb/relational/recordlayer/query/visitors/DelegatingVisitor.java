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
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperators;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

@API(API.Status.EXPERIMENTAL)
public class DelegatingVisitor<D extends TypedVisitor> implements TypedVisitor {

    @Nonnull
    private final D delegate;

    public DelegatingVisitor(@Nonnull D delegate) {
        this.delegate = delegate;
    }

    @Nonnull
    public D getDelegate() {
        return delegate;
    }

    @Nonnull
    @Override
    public Object visitRoot(@Nonnull RelationalParser.RootContext ctx) {
        return getDelegate().visitRoot(ctx);
    }

    @Nonnull
    @Override
    public Object visitStatements(@Nonnull RelationalParser.StatementsContext ctx) {
        return getDelegate().visitStatements(ctx);
    }

    @Nonnull
    @Override
    public Object visitStatement(@Nonnull RelationalParser.StatementContext ctx) {
        return getDelegate().visitStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx) {
        return getDelegate().visitDmlStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitDdlStatement(RelationalParser.DdlStatementContext ctx) {
        return getDelegate().visitDdlStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitTransactionStatement(@Nonnull RelationalParser.TransactionStatementContext ctx) {
        return getDelegate().visitTransactionStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitPreparedStatement(@Nonnull RelationalParser.PreparedStatementContext ctx) {
        return getDelegate().visitPreparedStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitAdministrationStatement(@Nonnull RelationalParser.AdministrationStatementContext ctx) {
        return getDelegate().visitAdministrationStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitUtilityStatement(@Nonnull RelationalParser.UtilityStatementContext ctx) {
        return getDelegate().visitUtilityStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitTemplateClause(@Nonnull RelationalParser.TemplateClauseContext ctx) {
        return getDelegate().visitTemplateClause(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx) {
        return getDelegate().visitCreateSchemaStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        return getDelegate().visitCreateSchemaTemplateStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx) {
        return getDelegate().visitCreateDatabaseStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitOptionsClause(@Nonnull RelationalParser.OptionsClauseContext ctx) {
        return getDelegate().visitOptionsClause(ctx);
    }

    @Nonnull
    @Override
    public Object visitOption(@Nonnull RelationalParser.OptionContext ctx) {
        return getDelegate().visitOption(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx) {
        return getDelegate().visitDropDatabaseStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return getDelegate().visitDropSchemaTemplateStatement(ctx);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx) {
        return getDelegate().visitDropSchemaStatement(ctx);
    }

    @Nonnull
    @Override
    public RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx) {
        return getDelegate().visitStructDefinition(ctx);
    }

    @Nonnull
    @Override
    public RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx) {
        return getDelegate().visitTableDefinition(ctx);
    }

    @Nonnull
    @Override
    public Object visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx) {
        return getDelegate().visitColumnDefinition(ctx);
    }

    @Nonnull
    @Override
    public DataType visitFunctionColumnType(@Nonnull final RelationalParser.FunctionColumnTypeContext ctx) {
        return getDelegate().visitFunctionColumnType(ctx);
    }

    @Nonnull
    @Override
    public DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx) {
        return getDelegate().visitColumnType(ctx);
    }

    @Nonnull
    @Override
    public Object visitPrimitiveType(@Nonnull RelationalParser.PrimitiveTypeContext ctx) {
        return getDelegate().visitPrimitiveType(ctx);
    }

    @Nonnull
    @Override
    public Object visitVectorType(final RelationalParser.VectorTypeContext ctx) {
        return getDelegate().visitVectorType(ctx);
    }

    @Override
    public Object visitVectorElementType(final RelationalParser.VectorElementTypeContext ctx) {
        return getDelegate().visitVectorElementType(ctx);
    }

    @Nonnull
    @Override
    public Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx) {
        return getDelegate().visitNullColumnConstraint(ctx);
    }

    @Nonnull
    @Override
    public Object visitPrimaryKeyDefinition(@Nonnull RelationalParser.PrimaryKeyDefinitionContext ctx) {
        return getDelegate().visitPrimaryKeyDefinition(ctx);
    }

    @Nonnull
    @Override
    public List<Identifier> visitFullIdList(@Nonnull RelationalParser.FullIdListContext ctx) {
        return getDelegate().visitFullIdList(ctx);
    }

    @Nonnull
    @Override
    public DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx) {
        return getDelegate().visitEnumDefinition(ctx);
    }

    @Nonnull
    @Override
    public RecordLayerIndex visitIndexDefinition(@Nonnull RelationalParser.IndexDefinitionContext ctx) {
        return getDelegate().visitIndexDefinition(ctx);
    }

    @Nonnull
    @Override
    public Object visitIndexAttributes(@Nonnull RelationalParser.IndexAttributesContext ctx) {
        return getDelegate().visitIndexAttributes(ctx);
    }

    @Nonnull
    @Override
    public Object visitIndexAttribute(@Nonnull RelationalParser.IndexAttributeContext ctx) {
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
    public CompiledSqlFunction visitTempSqlInvokedFunction(final RelationalParser.TempSqlInvokedFunctionContext ctx) {
        return getDelegate().visitTempSqlInvokedFunction(ctx);
    }

    @Override
    public UserDefinedFunction visitSqlInvokedFunction(@Nonnull RelationalParser.SqlInvokedFunctionContext ctx) {
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
    public Object visitExpressionBody(final RelationalParser.ExpressionBodyContext ctx) {
        return getDelegate().visitExpressionBody(ctx);
    }

    @Override
    public RecordLayerInvokedRoutine visitFunctionSpecification(final RelationalParser.FunctionSpecificationContext ctx) {
        return getDelegate().visitFunctionSpecification(ctx);
    }

    @Override
    public LogicalOperator visitSqlReturnStatement(final RelationalParser.SqlReturnStatementContext ctx) {
        return getDelegate().visitSqlReturnStatement(ctx);
    }

    @Override
    public LogicalOperator visitReturnValue(final RelationalParser.ReturnValueContext ctx) {
        return getDelegate().visitReturnValue(ctx);
    }

    @Nonnull
    @Override
    public Object visitCharSet(@Nonnull RelationalParser.CharSetContext ctx) {
        return getDelegate().visitCharSet(ctx);
    }

    @Nonnull
    @Override
    public Object visitIntervalType(@Nonnull RelationalParser.IntervalTypeContext ctx) {
        return getDelegate().visitIntervalType(ctx);
    }

    @Nonnull
    @Override
    public Object visitSchemaId(@Nonnull RelationalParser.SchemaIdContext ctx) {
        return getDelegate().visitSchemaId(ctx);
    }

    @Nonnull
    @Override
    public Object visitPath(@Nonnull RelationalParser.PathContext ctx) {
        return getDelegate().visitPath(ctx);
    }

    @Nonnull
    @Override
    public Object visitSchemaTemplateId(@Nonnull RelationalParser.SchemaTemplateIdContext ctx) {
        return getDelegate().visitSchemaTemplateId(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitDeleteStatement(@Nonnull RelationalParser.DeleteStatementContext ctx) {
        return getDelegate().visitDeleteStatement(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatement(@Nonnull RelationalParser.InsertStatementContext ctx) {
        return getDelegate().visitInsertStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitSelectStatement(@Nonnull RelationalParser.SelectStatementContext ctx) {
        return getDelegate().visitSelectStatement(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitQuery(@Nonnull RelationalParser.QueryContext ctx) {
        return getDelegate().visitQuery(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperators visitCtes(@Nonnull RelationalParser.CtesContext ctx) {
        return getDelegate().visitCtes(ctx);
    }

    @Override
    public Object visitTraversalOrderClause(final RelationalParser.TraversalOrderClauseContext ctx) {
        return getDelegate().visitTraversalOrderClause(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitNamedQuery(@Nonnull RelationalParser.NamedQueryContext ctx) {
        return getDelegate().visitNamedQuery(ctx);
    }

    @Override
    public LogicalOperator visitTableFunction(@Nonnull RelationalParser.TableFunctionContext ctx) {
        return getDelegate().visitTableFunction(ctx);
    }

    @Override
    public Expressions visitTableFunctionArgs(@Nonnull RelationalParser.TableFunctionArgsContext ctx) {
        return getDelegate().visitTableFunctionArgs(ctx);
    }

    @Override
    public Identifier visitTableFunctionName(@Nonnull RelationalParser.TableFunctionNameContext ctx) {
        return getDelegate().visitTableFunctionName(ctx);
    }

    @Nonnull
    @Override
    public Expression visitContinuation(@Nonnull RelationalParser.ContinuationContext ctx) {
        return getDelegate().visitContinuation(ctx);
    }

    @Nonnull
    @Override
    public Expression visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx) {
        return getDelegate().visitContinuationAtom(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitQueryTermDefault(@Nonnull RelationalParser.QueryTermDefaultContext ctx) {
        return getDelegate().visitQueryTermDefault(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitSetQuery(RelationalParser.SetQueryContext ctx) {
        return getDelegate().visitSetQuery(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatementValueSelect(@Nonnull RelationalParser.InsertStatementValueSelectContext ctx) {
        return getDelegate().visitInsertStatementValueSelect(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInsertStatementValueValues(@Nonnull RelationalParser.InsertStatementValueValuesContext ctx) {
        return getDelegate().visitInsertStatementValueValues(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitUpdatedElement(@Nonnull RelationalParser.UpdatedElementContext ctx) {
        return getDelegate().visitUpdatedElement(ctx);
    }

    @Nonnull
    @Override
    public Object visitAssignmentField(@Nonnull RelationalParser.AssignmentFieldContext ctx) {
        return getDelegate().visitAssignmentField(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitUpdateStatement(@Nonnull RelationalParser.UpdateStatementContext ctx) {
        return getDelegate().visitUpdateStatement(ctx);
    }

    @Nonnull
    @Override
    public List<OrderByExpression> visitOrderByClause(@Nonnull RelationalParser.OrderByClauseContext ctx) {
        return getDelegate().visitOrderByClause(ctx);
    }

    @Nonnull
    @Override
    public OrderByExpression visitOrderByExpression(@Nonnull RelationalParser.OrderByExpressionContext ctx) {
        return getDelegate().visitOrderByExpression(ctx);
    }

    @Override
    @Nullable
    public Void visitTableSources(@Nonnull RelationalParser.TableSourcesContext ctx) {
        return getDelegate().visitTableSources(ctx);
    }

    @Nullable
    @Override
    public Void visitTableSourceBase(@Nonnull RelationalParser.TableSourceBaseContext ctx) {
        return getDelegate().visitTableSourceBase(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitAtomTableItem(@Nonnull RelationalParser.AtomTableItemContext ctx) {
        return getDelegate().visitAtomTableItem(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitSubqueryTableItem(@Nonnull RelationalParser.SubqueryTableItemContext ctx) {
        return getDelegate().visitSubqueryTableItem(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitInlineTableItem(@Nonnull final RelationalParser.InlineTableItemContext ctx) {
        return getDelegate().visitInlineTableItem(ctx);
    }

    @Override
    public LogicalOperator visitTableValuedFunction(@Nonnull final RelationalParser.TableValuedFunctionContext ctx) {
        return getDelegate().visitTableValuedFunction(ctx);
    }

    @Nonnull
    @Override
    public Set<String> visitIndexHint(@Nonnull RelationalParser.IndexHintContext ctx) {
        return getDelegate().visitIndexHint(ctx);
    }

    @Nonnull
    @Override
    public Object visitIndexHintType(@Nonnull RelationalParser.IndexHintTypeContext ctx) {
        return getDelegate().visitIndexHintType(ctx);
    }

    @Nonnull
    @Override
    public NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> visitInlineTableDefinition(@Nonnull RelationalParser.InlineTableDefinitionContext ctx) {
        return getDelegate().visitInlineTableDefinition(ctx);
    }


    @Nullable
    @Override
    public Object visitInnerJoin(@Nonnull RelationalParser.InnerJoinContext ctx) {
        return getDelegate().visitInnerJoin(ctx);
    }

    @Nonnull
    @Override
    public Object visitStraightJoin(@Nonnull RelationalParser.StraightJoinContext ctx) {
        return getDelegate().visitStraightJoin(ctx);
    }

    @Nonnull
    @Override
    public Object visitOuterJoin(@Nonnull RelationalParser.OuterJoinContext ctx) {
        return getDelegate().visitOuterJoin(ctx);
    }

    @Nonnull
    @Override
    public Object visitNaturalJoin(@Nonnull RelationalParser.NaturalJoinContext ctx) {
        return getDelegate().visitNaturalJoin(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitSimpleTable(@Nonnull RelationalParser.SimpleTableContext ctx) {
        return getDelegate().visitSimpleTable(ctx);
    }

    @Nonnull
    @Override
    public LogicalOperator visitParenthesisQuery(RelationalParser.ParenthesisQueryContext ctx) {
        return getDelegate().visitParenthesisQuery(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx) {
        return getDelegate().visitSelectElements(ctx);
    }

    @Nonnull
    @Override
    public Expression visitSelectStarElement(@Nonnull RelationalParser.SelectStarElementContext ctx) {
        return getDelegate().visitSelectStarElement(ctx);
    }

    @Nonnull
    @Override
    public Object visitSelectQualifierStarElement(@Nonnull RelationalParser.SelectQualifierStarElementContext ctx) {
        return getDelegate().visitSelectQualifierStarElement(ctx);
    }

    @Nonnull
    @Override
    public Expression visitSelectExpressionElement(@Nonnull RelationalParser.SelectExpressionElementContext ctx) {
        return getDelegate().visitSelectExpressionElement(ctx);
    }

    @Override
    @Nullable
    public Void visitFromClause(@Nonnull RelationalParser.FromClauseContext ctx) {
        return getDelegate().visitFromClause(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitGroupByClause(@Nonnull RelationalParser.GroupByClauseContext ctx) {
        return getDelegate().visitGroupByClause(ctx);
    }

    @Nonnull
    @Override
    public Expression visitWhereExpr(@Nonnull RelationalParser.WhereExprContext ctx) {
        return getDelegate().visitWhereExpr(ctx);
    }

    @Nonnull
    @Override
    public Expression visitHavingClause(@Nonnull RelationalParser.HavingClauseContext ctx) {
        return getDelegate().visitHavingClause(ctx);
    }

    @Nonnull
    @Override
    public Expression visitGroupByItem(@Nonnull RelationalParser.GroupByItemContext ctx) {
        return getDelegate().visitGroupByItem(ctx);
    }

    @Nonnull
    @Override
    public Expression visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx) {
        return getDelegate().visitLimitClause(ctx);
    }

    @Nonnull
    @Override
    public Expression visitLimitClauseAtom(@Nonnull RelationalParser.LimitClauseAtomContext ctx) {
        return getDelegate().visitLimitClauseAtom(ctx);
    }

    @Nonnull
    @Override
    public Object visitQueryOptions(@Nonnull RelationalParser.QueryOptionsContext ctx) {
        return getDelegate().visitQueryOptions(ctx);
    }

    @Nonnull
    @Override
    public Object visitQueryOption(@Nonnull RelationalParser.QueryOptionContext ctx) {
        return getDelegate().visitQueryOption(ctx);
    }

    @Nonnull
    @Override
    public Object visitStartTransaction(@Nonnull RelationalParser.StartTransactionContext ctx) {
        return getDelegate().visitStartTransaction(ctx);
    }

    @Nonnull
    @Override
    public Object visitCommitStatement(@Nonnull RelationalParser.CommitStatementContext ctx) {
        return getDelegate().visitCommitStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitRollbackStatement(@Nonnull RelationalParser.RollbackStatementContext ctx) {
        return getDelegate().visitRollbackStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetAutocommitStatement(@Nonnull RelationalParser.SetAutocommitStatementContext ctx) {
        return getDelegate().visitSetAutocommitStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetTransactionStatement(@Nonnull RelationalParser.SetTransactionStatementContext ctx) {
        return getDelegate().visitSetTransactionStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitTransactionOption(@Nonnull RelationalParser.TransactionOptionContext ctx) {
        return getDelegate().visitTransactionOption(ctx);
    }

    @Nonnull
    @Override
    public Object visitTransactionLevel(@Nonnull RelationalParser.TransactionLevelContext ctx) {
        return getDelegate().visitTransactionLevel(ctx);
    }

    @Nonnull
    @Override
    public Object visitPrepareStatement(@Nonnull RelationalParser.PrepareStatementContext ctx) {
        return getDelegate().visitPrepareStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitExecuteStatement(@Nonnull RelationalParser.ExecuteStatementContext ctx) {
        return getDelegate().visitExecuteStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(@Nonnull RelationalParser.ShowDatabasesStatementContext ctx) {
        return getDelegate().visitShowDatabasesStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(@Nonnull RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        return getDelegate().visitShowSchemaTemplatesStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetVariable(@Nonnull RelationalParser.SetVariableContext ctx) {
        return getDelegate().visitSetVariable(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetCharset(@Nonnull RelationalParser.SetCharsetContext ctx) {
        return getDelegate().visitSetCharset(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetNames(@Nonnull RelationalParser.SetNamesContext ctx) {
        return getDelegate().visitSetNames(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetTransaction(@Nonnull RelationalParser.SetTransactionContext ctx) {
        return getDelegate().visitSetTransaction(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetAutocommit(@Nonnull RelationalParser.SetAutocommitContext ctx) {
        return getDelegate().visitSetAutocommit(ctx);
    }

    @Nonnull
    @Override
    public Object visitSetNewValueInsideTrigger(@Nonnull RelationalParser.SetNewValueInsideTriggerContext ctx) {
        return getDelegate().visitSetNewValueInsideTrigger(ctx);
    }

    @Nonnull
    @Override
    public Object visitVariableClause(@Nonnull RelationalParser.VariableClauseContext ctx) {
        return getDelegate().visitVariableClause(ctx);
    }

    @Nonnull
    @Override
    public Object visitKillStatement(@Nonnull RelationalParser.KillStatementContext ctx) {
        return getDelegate().visitKillStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitResetStatement(@Nonnull RelationalParser.ResetStatementContext ctx) {
        return getDelegate().visitResetStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx) {
        return getDelegate().visitExecuteContinuationStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitTableIndexes(@Nonnull RelationalParser.TableIndexesContext ctx) {
        return getDelegate().visitTableIndexes(ctx);
    }

    @Nonnull
    @Override
    public Object visitLoadedTableIndexes(@Nonnull RelationalParser.LoadedTableIndexesContext ctx) {
        return getDelegate().visitLoadedTableIndexes(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(@Nonnull RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        return getDelegate().visitSimpleDescribeSchemaStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(@Nonnull RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        return getDelegate().visitSimpleDescribeSchemaTemplateStatement(ctx);
    }

    @Nonnull
    @Override
    public QueryPlan.LogicalQueryPlan visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx) {
        return getDelegate().visitFullDescribeStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitHelpStatement(@Nonnull RelationalParser.HelpStatementContext ctx) {
        return getDelegate().visitHelpStatement(ctx);
    }

    @Nonnull
    @Override
    public Object visitDescribeStatements(@Nonnull RelationalParser.DescribeStatementsContext ctx) {
        return getDelegate().visitDescribeStatements(ctx);
    }

    @Nonnull
    @Override
    public Object visitDescribeConnection(@Nonnull RelationalParser.DescribeConnectionContext ctx) {
        return getDelegate().visitDescribeConnection(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitFullId(@Nonnull RelationalParser.FullIdContext ctx) {
        return getDelegate().visitFullId(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitUserDefinedScalarFunctionStatementBody(@Nonnull RelationalParser.UserDefinedScalarFunctionStatementBodyContext ctx) {
        return getDelegate().visitUserDefinedScalarFunctionStatementBody(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitTableName(@Nonnull RelationalParser.TableNameContext ctx) {
        return getDelegate().visitTableName(ctx);
    }

    @Nonnull
    @Override
    public Expression visitFullColumnName(@Nonnull RelationalParser.FullColumnNameContext ctx) {
        return getDelegate().visitFullColumnName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitIndexColumnName(@Nonnull RelationalParser.IndexColumnNameContext ctx) {
        return getDelegate().visitIndexColumnName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitCharsetName(@Nonnull RelationalParser.CharsetNameContext ctx) {
        return getDelegate().visitCharsetName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitCollationName(@Nonnull RelationalParser.CollationNameContext ctx) {
        return getDelegate().visitCollationName(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitUid(@Nonnull RelationalParser.UidContext ctx) {
        return getDelegate().visitUid(ctx);
    }

    @Nonnull
    @Override
    public Identifier visitSimpleId(@Nonnull RelationalParser.SimpleIdContext ctx) {
        return getDelegate().visitSimpleId(ctx);
    }

    @Nonnull
    @Override
    public Object visitNullNotnull(@Nonnull RelationalParser.NullNotnullContext ctx) {
        return getDelegate().visitNullNotnull(ctx);
    }

    @Nonnull
    @Override
    public Expression visitDecimalLiteral(@Nonnull RelationalParser.DecimalLiteralContext ctx) {
        return getDelegate().visitDecimalLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitStringLiteral(@Nonnull RelationalParser.StringLiteralContext ctx) {
        return getDelegate().visitStringLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBooleanLiteral(@Nonnull RelationalParser.BooleanLiteralContext ctx) {
        return getDelegate().visitBooleanLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBytesLiteral(@Nonnull RelationalParser.BytesLiteralContext ctx) {
        return getDelegate().visitBytesLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNullLiteral(@Nonnull RelationalParser.NullLiteralContext ctx) {
        return getDelegate().visitNullLiteral(ctx);
    }

    @Nonnull
    @Override
    public Expression visitStringConstant(@Nonnull RelationalParser.StringConstantContext ctx) {
        return getDelegate().visitStringConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitDecimalConstant(@Nonnull RelationalParser.DecimalConstantContext ctx) {
        return getDelegate().visitDecimalConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNegativeDecimalConstant(@Nonnull RelationalParser.NegativeDecimalConstantContext ctx) {
        return getDelegate().visitNegativeDecimalConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBytesConstant(@Nonnull RelationalParser.BytesConstantContext ctx) {
        return getDelegate().visitBytesConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBooleanConstant(@Nonnull RelationalParser.BooleanConstantContext ctx) {
        return getDelegate().visitBooleanConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBitStringConstant(@Nonnull RelationalParser.BitStringConstantContext ctx) {
        return getDelegate().visitBitStringConstant(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNullConstant(@Nonnull RelationalParser.NullConstantContext ctx) {
        return getDelegate().visitNullConstant(ctx);
    }

    @Nonnull
    @Override
    public Object visitStringDataType(@Nonnull RelationalParser.StringDataTypeContext ctx) {
        return getDelegate().visitStringDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitNationalStringDataType(@Nonnull RelationalParser.NationalStringDataTypeContext ctx) {
        return getDelegate().visitNationalStringDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitNationalVaryingStringDataType(@Nonnull RelationalParser.NationalVaryingStringDataTypeContext ctx) {
        return getDelegate().visitNationalVaryingStringDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitDimensionDataType(@Nonnull RelationalParser.DimensionDataTypeContext ctx) {
        return getDelegate().visitDimensionDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitSimpleDataType(@Nonnull RelationalParser.SimpleDataTypeContext ctx) {
        return getDelegate().visitSimpleDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitCollectionDataType(@Nonnull RelationalParser.CollectionDataTypeContext ctx) {
        return getDelegate().visitCollectionDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitSpatialDataType(@Nonnull RelationalParser.SpatialDataTypeContext ctx) {
        return getDelegate().visitSpatialDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitLongVarcharDataType(@Nonnull RelationalParser.LongVarcharDataTypeContext ctx) {
        return getDelegate().visitLongVarcharDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitLongVarbinaryDataType(@Nonnull RelationalParser.LongVarbinaryDataTypeContext ctx) {
        return getDelegate().visitLongVarbinaryDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitCollectionOptions(@Nonnull RelationalParser.CollectionOptionsContext ctx) {
        return getDelegate().visitCollectionOptions(ctx);
    }

    @Nonnull
    @Override
    public Object visitConvertedDataType(@Nonnull RelationalParser.ConvertedDataTypeContext ctx) {
        return getDelegate().visitConvertedDataType(ctx);
    }

    @Nonnull
    @Override
    public Object visitLengthOneDimension(@Nonnull RelationalParser.LengthOneDimensionContext ctx) {
        return getDelegate().visitLengthOneDimension(ctx);
    }

    @Nonnull
    @Override
    public Object visitLengthTwoDimension(@Nonnull RelationalParser.LengthTwoDimensionContext ctx) {
        return getDelegate().visitLengthTwoDimension(ctx);
    }

    @Nonnull
    @Override
    public Object visitLengthTwoOptionalDimension(@Nonnull RelationalParser.LengthTwoOptionalDimensionContext ctx) {
        return getDelegate().visitLengthTwoOptionalDimension(ctx);
    }

    @Nonnull
    @Override
    public List<Identifier> visitUidList(@Nonnull RelationalParser.UidListContext ctx) {
        return getDelegate().visitUidList(ctx);
    }

    @Nonnull
    @Override
    public Object visitUidWithNestings(@Nonnull RelationalParser.UidWithNestingsContext ctx) {
        return getDelegate().visitUidWithNestings(ctx);
    }

    @Nonnull
    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestingsInParens(@Nonnull RelationalParser.UidListWithNestingsInParensContext ctx) {
        return getDelegate().visitUidListWithNestingsInParens(ctx);
    }

    @Nonnull
    @Override
    public CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestings(@Nonnull RelationalParser.UidListWithNestingsContext ctx) {
        return getDelegate().visitUidListWithNestings(ctx);
    }

    @Nonnull
    @Override
    public Object visitTables(@Nonnull RelationalParser.TablesContext ctx) {
        return getDelegate().visitTables(ctx);
    }

    @Nonnull
    @Override
    public Object visitIndexColumnNames(@Nonnull RelationalParser.IndexColumnNamesContext ctx) {
        return getDelegate().visitIndexColumnNames(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitExpressions(@Nonnull RelationalParser.ExpressionsContext ctx) {
        return getDelegate().visitExpressions(ctx);
    }

    @Nonnull
    @Override
    public Object visitExpressionsWithDefaults(@Nonnull RelationalParser.ExpressionsWithDefaultsContext ctx) {
        return getDelegate().visitExpressionsWithDefaults(ctx);
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructorForInsert(@Nonnull RelationalParser.RecordConstructorForInsertContext ctx) {
        return getDelegate().visitRecordConstructorForInsert(ctx);
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructorForInlineTable(@Nonnull RelationalParser.RecordConstructorForInlineTableContext ctx) {
        return getDelegate().visitRecordConstructorForInlineTable(ctx);
    }

    @Nonnull
    @Override
    public Expression visitRecordConstructor(@Nonnull RelationalParser.RecordConstructorContext ctx) {
        return getDelegate().visitRecordConstructor(ctx);
    }

    @Nonnull
    @Override
    public Object visitOfTypeClause(@Nonnull RelationalParser.OfTypeClauseContext ctx) {
        return getDelegate().visitOfTypeClause(ctx);
    }

    @Nonnull
    @Override
    public Expression visitArrayConstructor(@Nonnull RelationalParser.ArrayConstructorContext ctx) {
        return getDelegate().visitArrayConstructor(ctx);
    }

    @Nonnull
    @Override
    public Object visitUserVariables(@Nonnull RelationalParser.UserVariablesContext ctx) {
        return getDelegate().visitUserVariables(ctx);
    }

    @Nonnull
    @Override
    public Object visitDefaultValue(@Nonnull RelationalParser.DefaultValueContext ctx) {
        return getDelegate().visitDefaultValue(ctx);
    }

    @Nonnull
    @Override
    public Object visitCurrentTimestamp(@Nonnull RelationalParser.CurrentTimestampContext ctx) {
        return getDelegate().visitCurrentTimestamp(ctx);
    }

    @Nonnull
    @Override
    public Object visitExpressionOrDefault(@Nonnull RelationalParser.ExpressionOrDefaultContext ctx) {
        return getDelegate().visitExpressionOrDefault(ctx);
    }

    @Nonnull
    @Override
    public Expression visitExpressionWithOptionalName(@Nonnull RelationalParser.ExpressionWithOptionalNameContext ctx) {
        return getDelegate().visitExpressionWithOptionalName(ctx);
    }

    @Nonnull
    @Override
    public Object visitIfExists(@Nonnull RelationalParser.IfExistsContext ctx) {
        return getDelegate().visitIfExists(ctx);
    }

    @Nonnull
    @Override
    public Object visitIfNotExists(@Nonnull RelationalParser.IfNotExistsContext ctx) {
        return getDelegate().visitIfNotExists(ctx);
    }

    @Nonnull
    @Override
    public Object visitUserDefinedScalarFunctionName(@Nonnull RelationalParser.UserDefinedScalarFunctionNameContext ctx) {
        return getDelegate().visitUserDefinedScalarFunctionName(ctx);
    }

    @Nonnull
    @Override
    public Expression visitUserDefinedScalarFunctionCall(@Nonnull RelationalParser.UserDefinedScalarFunctionCallContext ctx) {
        return getDelegate().visitUserDefinedScalarFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Expression visitAggregateFunctionCall(@Nonnull RelationalParser.AggregateFunctionCallContext ctx) {
        return getDelegate().visitAggregateFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitSpecificFunctionCall(@Nonnull RelationalParser.SpecificFunctionCallContext ctx) {
        return getDelegate().visitSpecificFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Expression visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx) {
        return getDelegate().visitScalarFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitSimpleFunctionCall(@Nonnull RelationalParser.SimpleFunctionCallContext ctx) {
        return getDelegate().visitSimpleFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitDataTypeFunctionCall(@Nonnull RelationalParser.DataTypeFunctionCallContext ctx) {
        return getDelegate().visitDataTypeFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitValuesFunctionCall(@Nonnull RelationalParser.ValuesFunctionCallContext ctx) {
        return getDelegate().visitValuesFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitCaseExpressionFunctionCall(@Nonnull RelationalParser.CaseExpressionFunctionCallContext ctx) {
        return getDelegate().visitCaseExpressionFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Expression visitCaseFunctionCall(@Nonnull RelationalParser.CaseFunctionCallContext ctx) {
        return getDelegate().visitCaseFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitCharFunctionCall(@Nonnull RelationalParser.CharFunctionCallContext ctx) {
        return getDelegate().visitCharFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitPositionFunctionCall(@Nonnull RelationalParser.PositionFunctionCallContext ctx) {
        return getDelegate().visitPositionFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitSubstrFunctionCall(@Nonnull RelationalParser.SubstrFunctionCallContext ctx) {
        return getDelegate().visitSubstrFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitTrimFunctionCall(@Nonnull RelationalParser.TrimFunctionCallContext ctx) {
        return getDelegate().visitTrimFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitWeightFunctionCall(@Nonnull RelationalParser.WeightFunctionCallContext ctx) {
        return getDelegate().visitWeightFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitExtractFunctionCall(@Nonnull RelationalParser.ExtractFunctionCallContext ctx) {
        return getDelegate().visitExtractFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitGetFormatFunctionCall(@Nonnull RelationalParser.GetFormatFunctionCallContext ctx) {
        return getDelegate().visitGetFormatFunctionCall(ctx);
    }

    @Nonnull
    @Override
    public Object visitCaseFuncAlternative(@Nonnull RelationalParser.CaseFuncAlternativeContext ctx) {
        return getDelegate().visitCaseFuncAlternative(ctx);
    }

    @Nonnull
    @Override
    public Object visitLevelWeightList(@Nonnull RelationalParser.LevelWeightListContext ctx) {
        return getDelegate().visitLevelWeightList(ctx);
    }

    @Nonnull
    @Override
    public Object visitLevelWeightRange(@Nonnull RelationalParser.LevelWeightRangeContext ctx) {
        return getDelegate().visitLevelWeightRange(ctx);
    }

    @Nonnull
    @Override
    public Object visitLevelInWeightListElement(@Nonnull RelationalParser.LevelInWeightListElementContext ctx) {
        return getDelegate().visitLevelInWeightListElement(ctx);
    }

    @Nonnull
    @Override
    public Expression visitAggregateWindowedFunction(@Nonnull RelationalParser.AggregateWindowedFunctionContext ctx) {
        return getDelegate().visitAggregateWindowedFunction(ctx);
    }

    @Nonnull
    @Override
    public Object visitNonAggregateWindowedFunction(@Nonnull RelationalParser.NonAggregateWindowedFunctionContext ctx) {
        return getDelegate().visitNonAggregateWindowedFunction(ctx);
    }

    @Nonnull
    @Override
    public Object visitOverClause(@Nonnull RelationalParser.OverClauseContext ctx) {
        return getDelegate().visitOverClause(ctx);
    }

    @Nonnull
    @Override
    public Object visitWindowName(@Nonnull RelationalParser.WindowNameContext ctx) {
        return getDelegate().visitWindowName(ctx);
    }

    @Nonnull
    @Override
    public Object visitScalarFunctionName(@Nonnull RelationalParser.ScalarFunctionNameContext ctx) {
        return getDelegate().visitScalarFunctionName(ctx);
    }

    @Nonnull
    @Override
    public Expressions visitFunctionArgs(@Nonnull RelationalParser.FunctionArgsContext ctx) {
        return getDelegate().visitFunctionArgs(ctx);
    }

    @Nonnull
    @Override
    public Object visitFunctionArg(@Nonnull RelationalParser.FunctionArgContext ctx) {
        return getDelegate().visitFunctionArg(ctx);
    }

    @Override
    public Expression visitNamedFunctionArg(final RelationalParser.NamedFunctionArgContext ctx) {
        return getDelegate().visitNamedFunctionArg(ctx);
    }

    @Nonnull
    @Override
    public Expression visitNotExpression(@Nonnull RelationalParser.NotExpressionContext ctx) {
        return getDelegate().visitNotExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitLogicalExpression(@Nonnull RelationalParser.LogicalExpressionContext ctx) {
        return getDelegate().visitLogicalExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitPredicatedExpression(@Nonnull RelationalParser.PredicatedExpressionContext ctx) {
        return getDelegate().visitPredicatedExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBinaryComparisonPredicate(@Nonnull RelationalParser.BinaryComparisonPredicateContext ctx) {
        return getDelegate().visitBinaryComparisonPredicate(ctx);
    }

    @Override
    public Expression visitSubscriptExpression(@Nonnull RelationalParser.SubscriptExpressionContext ctx) {
        return getDelegate().visitSubscriptExpression(ctx);
    }

    @Nonnull
    @Override
    public Expression visitInList(@Nonnull RelationalParser.InListContext ctx) {
        return getDelegate().visitInList(ctx);
    }

    @Nonnull
    @Override
    public Object visitConstantExpressionAtom(@Nonnull RelationalParser.ConstantExpressionAtomContext ctx) {
        return getDelegate().visitConstantExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitFunctionCallExpressionAtom(@Nonnull RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return getDelegate().visitFunctionCallExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Object visitFullColumnNameExpressionAtom(@Nonnull RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        return getDelegate().visitFullColumnNameExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitBitExpressionAtom(@Nonnull RelationalParser.BitExpressionAtomContext ctx) {
        return getDelegate().visitBitExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitPreparedStatementParameterAtom(@Nonnull RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return getDelegate().visitPreparedStatementParameterAtom(ctx);
    }

    @Nonnull
    @Override
    public Object visitRecordConstructorExpressionAtom(@Nonnull RelationalParser.RecordConstructorExpressionAtomContext ctx) {
        return getDelegate().visitRecordConstructorExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Object visitArrayConstructorExpressionAtom(@Nonnull RelationalParser.ArrayConstructorExpressionAtomContext ctx) {
        return getDelegate().visitArrayConstructorExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitMathExpressionAtom(@Nonnull RelationalParser.MathExpressionAtomContext ctx) {
        return getDelegate().visitMathExpressionAtom(ctx);
    }

    @Nonnull
    @Override
    public Expression visitExistsExpressionAtom(@Nonnull RelationalParser.ExistsExpressionAtomContext ctx) {
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

    @Nonnull
    @Override
    public Expression visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx) {
        return getDelegate().visitPreparedStatementParameter(ctx);
    }

    @Nonnull
    @Override
    public Object visitUnaryOperator(@Nonnull RelationalParser.UnaryOperatorContext ctx) {
        return getDelegate().visitUnaryOperator(ctx);
    }

    @Nonnull
    @Override
    public Object visitComparisonOperator(@Nonnull RelationalParser.ComparisonOperatorContext ctx) {
        return getDelegate().visitComparisonOperator(ctx);
    }

    @Nonnull
    @Override
    public Object visitLogicalOperator(@Nonnull RelationalParser.LogicalOperatorContext ctx) {
        return getDelegate().visitLogicalOperator(ctx);
    }

    @Nonnull
    @Override
    public Object visitBitOperator(@Nonnull RelationalParser.BitOperatorContext ctx) {
        return getDelegate().visitBitOperator(ctx);
    }

    @Nonnull
    @Override
    public Object visitMathOperator(@Nonnull RelationalParser.MathOperatorContext ctx) {
        return getDelegate().visitMathOperator(ctx);
    }

    @Nonnull
    @Override
    public Object visitJsonOperator(@Nonnull RelationalParser.JsonOperatorContext ctx) {
        return getDelegate().visitJsonOperator(ctx);
    }

    @Nonnull
    @Override
    public Object visitCharsetNameBase(@Nonnull RelationalParser.CharsetNameBaseContext ctx) {
        return getDelegate().visitCharsetNameBase(ctx);
    }

    @Nonnull
    @Override
    public Object visitIntervalTypeBase(@Nonnull RelationalParser.IntervalTypeBaseContext ctx) {
        return getDelegate().visitIntervalTypeBase(ctx);
    }

    @Nonnull
    @Override
    public Object visitKeywordsCanBeId(@Nonnull RelationalParser.KeywordsCanBeIdContext ctx) {
        return getDelegate().visitKeywordsCanBeId(ctx);
    }

    @Nonnull
    @Override
    public Object visitFunctionNameBase(@Nonnull RelationalParser.FunctionNameBaseContext ctx) {
        return getDelegate().visitFunctionNameBase(ctx);
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
    public Object visitTerminal(TerminalNode node) {
        return getDelegate().visitTerminal(node);
    }

    @Override
    public Object visitErrorNode(ErrorNode node) {
        return getDelegate().visitErrorNode(node);
    }
}
