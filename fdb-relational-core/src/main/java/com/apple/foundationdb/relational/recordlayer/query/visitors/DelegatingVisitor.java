/*
 * DelegatingVisitor.java
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

import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.StringTrieNode;

import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

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

    @Override
    @Nonnull
    public Object visitRoot(@Nonnull RelationalParser.RootContext ctx) {
        return getDelegate().visitRoot(ctx);
    }

    @Override
    @Nonnull
    public Object visitSqlStatements(@Nonnull RelationalParser.SqlStatementsContext ctx) {
        return getDelegate().visitSqlStatements(ctx);
    }

    @Override
    @Nonnull
    public Object visitSqlStatement(@Nonnull RelationalParser.SqlStatementContext ctx) {
        return getDelegate().visitSqlStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitDdlStatement(@Nonnull RelationalParser.DdlStatementContext ctx) {
        return getDelegate().visitDdlStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.LogicalQueryPlan visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx) {
        return getDelegate().visitDmlStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitTransactionStatement(@Nonnull RelationalParser.TransactionStatementContext ctx) {
        return getDelegate().visitTransactionStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitPreparedStatement(@Nonnull RelationalParser.PreparedStatementContext ctx) {
        return getDelegate().visitPreparedStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitAdministrationStatement(@Nonnull RelationalParser.AdministrationStatementContext ctx) {
        return getDelegate().visitAdministrationStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitUtilityStatement(@Nonnull RelationalParser.UtilityStatementContext ctx) {
        return getDelegate().visitUtilityStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitTemplateClause(@Nonnull RelationalParser.TemplateClauseContext ctx) {
        return getDelegate().visitTemplateClause(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx) {
        return getDelegate().visitCreateSchemaStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        return getDelegate().visitCreateSchemaTemplateStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx) {
        return getDelegate().visitCreateDatabaseStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitOptionsClause(@Nonnull RelationalParser.OptionsClauseContext ctx) {
        return getDelegate().visitOptionsClause(ctx);
    }

    @Override
    @Nonnull
    public Object visitOption(@Nonnull RelationalParser.OptionContext ctx) {
        return getDelegate().visitOption(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx) {
        return getDelegate().visitDropDatabaseStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx) {
        return getDelegate().visitDropSchemaTemplateStatement(ctx);
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx) {
        return getDelegate().visitDropSchemaStatement(ctx);
    }

    @Override
    @Nonnull
    public RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx) {
        return getDelegate().visitStructDefinition(ctx);
    }

    @Override
    @Nonnull
    public RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx) {
        return getDelegate().visitTableDefinition(ctx);
    }

    @Override
    @Nonnull
    public Object visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx) {
        return getDelegate().visitColumnDefinition(ctx);
    }

    @Override
    @Nonnull
    public DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx) {
        return getDelegate().visitColumnType(ctx);
    }

    @Override
    @Nonnull
    public DataType visitPrimitiveType(@Nonnull RelationalParser.PrimitiveTypeContext ctx) {
        return getDelegate().visitPrimitiveType(ctx);
    }

    @Override
    @Nonnull
    public Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx) {
        return getDelegate().visitNullColumnConstraint(ctx);
    }

    @Override
    @Nonnull
    public Object visitPrimaryKeyDefinition(@Nonnull RelationalParser.PrimaryKeyDefinitionContext ctx) {
        return getDelegate().visitPrimaryKeyDefinition(ctx);
    }

    @Override
    @Nonnull
    public DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx) {
        return getDelegate().visitEnumDefinition(ctx);
    }

    @Override
    @Nonnull
    public RecordLayerIndex visitIndexDefinition(@Nonnull RelationalParser.IndexDefinitionContext ctx) {
        return getDelegate().visitIndexDefinition(ctx);
    }

    @Override
    @Nonnull
    public Object visitCharSet(@Nonnull RelationalParser.CharSetContext ctx) {
        return getDelegate().visitCharSet(ctx);
    }

    @Override
    @Nonnull
    public Object visitIntervalType(@Nonnull RelationalParser.IntervalTypeContext ctx) {
        return getDelegate().visitIntervalType(ctx);
    }

    @Override
    @Nonnull
    public Object visitSchemaId(@Nonnull RelationalParser.SchemaIdContext ctx) {
        return getDelegate().visitSchemaId(ctx);
    }

    @Override
    @Nonnull
    public Object visitPath(@Nonnull RelationalParser.PathContext ctx) {
        return getDelegate().visitPath(ctx);
    }

    @Override
    @Nonnull
    public Object visitSchemaTemplateId(@Nonnull RelationalParser.SchemaTemplateIdContext ctx) {
        return getDelegate().visitSchemaTemplateId(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitDeleteStatement(@Nonnull RelationalParser.DeleteStatementContext ctx) {
        return getDelegate().visitDeleteStatement(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitInsertStatement(@Nonnull RelationalParser.InsertStatementContext ctx) {
        return getDelegate().visitInsertStatement(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitSelectStatementWithContinuation(@Nonnull RelationalParser.SelectStatementWithContinuationContext ctx) {
        return getDelegate().visitSelectStatementWithContinuation(ctx);
    }

    @Override
    @Nonnull
    public Expression visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx) {
        return getDelegate().visitContinuationAtom(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitSimpleSelect(@Nonnull RelationalParser.SimpleSelectContext ctx) {
        return getDelegate().visitSimpleSelect(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitParenthesisSelect(@Nonnull RelationalParser.ParenthesisSelectContext ctx) {
        return getDelegate().visitParenthesisSelect(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionSelect(@Nonnull RelationalParser.UnionSelectContext ctx) {
        return getDelegate().visitUnionSelect(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionParenthesisSelect(@Nonnull RelationalParser.UnionParenthesisSelectContext ctx) {
        return getDelegate().visitUnionParenthesisSelect(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitInsertStatementValueSelect(@Nonnull RelationalParser.InsertStatementValueSelectContext ctx) {
        return getDelegate().visitInsertStatementValueSelect(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitInsertStatementValueValues(@Nonnull RelationalParser.InsertStatementValueValuesContext ctx) {
        return getDelegate().visitInsertStatementValueValues(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitUpdatedElement(@Nonnull RelationalParser.UpdatedElementContext ctx) {
        return getDelegate().visitUpdatedElement(ctx);
    }

    @Override
    @Nonnull
    public Object visitAssignmentField(@Nonnull RelationalParser.AssignmentFieldContext ctx) {
        return getDelegate().visitAssignmentField(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUpdateStatement(@Nonnull RelationalParser.UpdateStatementContext ctx) {
        return getDelegate().visitUpdateStatement(ctx);
    }

    @Override
    @Nonnull
    public Pair<Boolean, Expressions> visitOrderByClause(@Nonnull RelationalParser.OrderByClauseContext ctx) {
        return getDelegate().visitOrderByClause(ctx);
    }

    @Override
    @Nonnull
    public Pair<Expression, Boolean> visitOrderByExpression(@Nonnull RelationalParser.OrderByExpressionContext ctx) {
        return getDelegate().visitOrderByExpression(ctx);
    }

    @Override
    @Nullable
    public Void visitTableSources(@Nonnull RelationalParser.TableSourcesContext ctx) {
        return getDelegate().visitTableSources(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitTableSourceBase(@Nonnull RelationalParser.TableSourceBaseContext ctx) {
        return getDelegate().visitTableSourceBase(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitAtomTableItem(@Nonnull RelationalParser.AtomTableItemContext ctx) {
        return getDelegate().visitAtomTableItem(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitSubqueryTableItem(@Nonnull RelationalParser.SubqueryTableItemContext ctx) {
        return getDelegate().visitSubqueryTableItem(ctx);
    }

    @Override
    @Nonnull
    public Set<String> visitIndexHint(@Nonnull RelationalParser.IndexHintContext ctx) {
        return getDelegate().visitIndexHint(ctx);
    }

    @Override
    @Nonnull
    public Object visitIndexHintType(@Nonnull RelationalParser.IndexHintTypeContext ctx) {
        return getDelegate().visitIndexHintType(ctx);
    }

    @Override
    @Nonnull
    public Object visitInnerJoin(@Nonnull RelationalParser.InnerJoinContext ctx) {
        return getDelegate().visitInnerJoin(ctx);
    }

    @Override
    @Nonnull
    public Object visitStraightJoin(@Nonnull RelationalParser.StraightJoinContext ctx) {
        return getDelegate().visitStraightJoin(ctx);
    }

    @Override
    @Nonnull
    public Object visitOuterJoin(@Nonnull RelationalParser.OuterJoinContext ctx) {
        return getDelegate().visitOuterJoin(ctx);
    }

    @Override
    @Nonnull
    public Object visitNaturalJoin(@Nonnull RelationalParser.NaturalJoinContext ctx) {
        return getDelegate().visitNaturalJoin(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitQueryExpression(@Nonnull RelationalParser.QueryExpressionContext ctx) {
        return getDelegate().visitQueryExpression(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitQuerySpecification(@Nonnull RelationalParser.QuerySpecificationContext ctx) {
        return getDelegate().visitQuerySpecification(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionParenthesis(@Nonnull RelationalParser.UnionParenthesisContext ctx) {
        return getDelegate().visitUnionParenthesis(ctx);
    }

    @Override
    @Nonnull
    public LogicalOperator visitUnionStatement(@Nonnull RelationalParser.UnionStatementContext ctx) {
        return getDelegate().visitUnionStatement(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx) {
        return getDelegate().visitSelectElements(ctx);
    }

    @Override
    @Nonnull
    public Expression visitSelectStarElement(@Nonnull RelationalParser.SelectStarElementContext ctx) {
        return getDelegate().visitSelectStarElement(ctx);
    }

    @Override
    @Nonnull
    public Object visitSelectQualifierStarElement(@Nonnull RelationalParser.SelectQualifierStarElementContext ctx) {
        return getDelegate().visitSelectQualifierStarElement(ctx);
    }

    @Override
    @Nonnull
    public Expression visitSelectExpressionElement(@Nonnull RelationalParser.SelectExpressionElementContext ctx) {
        return getDelegate().visitSelectExpressionElement(ctx);
    }

    @Override
    @Nullable
    public Void visitFromClause(@Nonnull RelationalParser.FromClauseContext ctx) {
        return getDelegate().visitFromClause(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitGroupByClause(@Nonnull RelationalParser.GroupByClauseContext ctx) {
        return getDelegate().visitGroupByClause(ctx);
    }

    @Override
    @Nonnull
    public Expression visitWhereExpr(@Nonnull RelationalParser.WhereExprContext ctx) {
        return getDelegate().visitWhereExpr(ctx);
    }

    @Override
    @Nonnull
    public Expression visitHavingClause(@Nonnull RelationalParser.HavingClauseContext ctx) {
        return getDelegate().visitHavingClause(ctx);
    }

    @Override
    @Nonnull
    public Expression visitGroupByItem(@Nonnull RelationalParser.GroupByItemContext ctx) {
        return getDelegate().visitGroupByItem(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx) {
        return getDelegate().visitLimitClause(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLimitClauseAtom(@Nonnull RelationalParser.LimitClauseAtomContext ctx) {
        return getDelegate().visitLimitClauseAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitQueryOptions(@Nonnull RelationalParser.QueryOptionsContext ctx) {
        return getDelegate().visitQueryOptions(ctx);
    }

    @Override
    @Nonnull
    public Object visitQueryOption(@Nonnull RelationalParser.QueryOptionContext ctx) {
        return getDelegate().visitQueryOption(ctx);
    }

    @Override
    @Nonnull
    public Object visitStartTransaction(@Nonnull RelationalParser.StartTransactionContext ctx) {
        return getDelegate().visitStartTransaction(ctx);
    }

    @Override
    @Nonnull
    public Object visitCommitStatement(@Nonnull RelationalParser.CommitStatementContext ctx) {
        return getDelegate().visitCommitStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitRollbackStatement(@Nonnull RelationalParser.RollbackStatementContext ctx) {
        return getDelegate().visitRollbackStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetAutocommitStatement(@Nonnull RelationalParser.SetAutocommitStatementContext ctx) {
        return getDelegate().visitSetAutocommitStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetTransactionStatement(@Nonnull RelationalParser.SetTransactionStatementContext ctx) {
        return getDelegate().visitSetTransactionStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitTransactionOption(@Nonnull RelationalParser.TransactionOptionContext ctx) {
        return getDelegate().visitTransactionOption(ctx);
    }

    @Override
    @Nonnull
    public Object visitTransactionLevel(@Nonnull RelationalParser.TransactionLevelContext ctx) {
        return getDelegate().visitTransactionLevel(ctx);
    }

    @Override
    @Nonnull
    public Object visitPrepareStatement(@Nonnull RelationalParser.PrepareStatementContext ctx) {
        return getDelegate().visitPrepareStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitExecuteStatement(@Nonnull RelationalParser.ExecuteStatementContext ctx) {
        return getDelegate().visitExecuteStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(@Nonnull RelationalParser.ShowDatabasesStatementContext ctx) {
        return getDelegate().visitShowDatabasesStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(@Nonnull RelationalParser.ShowSchemaTemplatesStatementContext ctx) {
        return getDelegate().visitShowSchemaTemplatesStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetVariable(@Nonnull RelationalParser.SetVariableContext ctx) {
        return getDelegate().visitSetVariable(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetCharset(@Nonnull RelationalParser.SetCharsetContext ctx) {
        return getDelegate().visitSetCharset(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetNames(@Nonnull RelationalParser.SetNamesContext ctx) {
        return getDelegate().visitSetNames(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetTransaction(@Nonnull RelationalParser.SetTransactionContext ctx) {
        return getDelegate().visitSetTransaction(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetAutocommit(@Nonnull RelationalParser.SetAutocommitContext ctx) {
        return getDelegate().visitSetAutocommit(ctx);
    }

    @Override
    @Nonnull
    public Object visitSetNewValueInsideTrigger(@Nonnull RelationalParser.SetNewValueInsideTriggerContext ctx) {
        return getDelegate().visitSetNewValueInsideTrigger(ctx);
    }

    @Override
    @Nonnull
    public Object visitVariableClause(@Nonnull RelationalParser.VariableClauseContext ctx) {
        return getDelegate().visitVariableClause(ctx);
    }

    @Override
    @Nonnull
    public Object visitKillStatement(@Nonnull RelationalParser.KillStatementContext ctx) {
        return getDelegate().visitKillStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitResetStatement(@Nonnull RelationalParser.ResetStatementContext ctx) {
        return getDelegate().visitResetStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx) {
        return getDelegate().visitExecuteContinuationStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitTableIndexes(@Nonnull RelationalParser.TableIndexesContext ctx) {
        return getDelegate().visitTableIndexes(ctx);
    }

    @Override
    @Nonnull
    public Object visitLoadedTableIndexes(@Nonnull RelationalParser.LoadedTableIndexesContext ctx) {
        return getDelegate().visitLoadedTableIndexes(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(@Nonnull RelationalParser.SimpleDescribeSchemaStatementContext ctx) {
        return getDelegate().visitSimpleDescribeSchemaStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(@Nonnull RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx) {
        return getDelegate().visitSimpleDescribeSchemaTemplateStatement(ctx);
    }

    @Override
    @Nonnull
    public QueryPlan.LogicalQueryPlan visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx) {
        return getDelegate().visitFullDescribeStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitHelpStatement(@Nonnull RelationalParser.HelpStatementContext ctx) {
        return getDelegate().visitHelpStatement(ctx);
    }

    @Override
    @Nonnull
    public Object visitDescribeStatements(@Nonnull RelationalParser.DescribeStatementsContext ctx) {
        return getDelegate().visitDescribeStatements(ctx);
    }

    @Override
    @Nonnull
    public Object visitDescribeConnection(@Nonnull RelationalParser.DescribeConnectionContext ctx) {
        return getDelegate().visitDescribeConnection(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitFullId(@Nonnull RelationalParser.FullIdContext ctx) {
        return getDelegate().visitFullId(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitTableName(@Nonnull RelationalParser.TableNameContext ctx) {
        return getDelegate().visitTableName(ctx);
    }

    @Override
    @Nonnull
    public Expression visitFullColumnName(@Nonnull RelationalParser.FullColumnNameContext ctx) {
        return getDelegate().visitFullColumnName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitIndexColumnName(@Nonnull RelationalParser.IndexColumnNameContext ctx) {
        return getDelegate().visitIndexColumnName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitCharsetName(@Nonnull RelationalParser.CharsetNameContext ctx) {
        return getDelegate().visitCharsetName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitCollationName(@Nonnull RelationalParser.CollationNameContext ctx) {
        return getDelegate().visitCollationName(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitUid(@Nonnull RelationalParser.UidContext ctx) {
        return getDelegate().visitUid(ctx);
    }

    @Override
    @Nonnull
    public Identifier visitSimpleId(@Nonnull RelationalParser.SimpleIdContext ctx) {
        return getDelegate().visitSimpleId(ctx);
    }

    @Override
    @Nonnull
    public Object visitNullNotnull(@Nonnull RelationalParser.NullNotnullContext ctx) {
        return getDelegate().visitNullNotnull(ctx);
    }

    @Override
    @Nonnull
    public Expression visitDecimalLiteral(@Nonnull RelationalParser.DecimalLiteralContext ctx) {
        return getDelegate().visitDecimalLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitStringLiteral(@Nonnull RelationalParser.StringLiteralContext ctx) {
        return getDelegate().visitStringLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBooleanLiteral(@Nonnull RelationalParser.BooleanLiteralContext ctx) {
        return getDelegate().visitBooleanLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBytesLiteral(@Nonnull RelationalParser.BytesLiteralContext ctx) {
        return getDelegate().visitBytesLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNullLiteral(@Nonnull RelationalParser.NullLiteralContext ctx) {
        return getDelegate().visitNullLiteral(ctx);
    }

    @Override
    @Nonnull
    public Expression visitStringConstant(@Nonnull RelationalParser.StringConstantContext ctx) {
        return getDelegate().visitStringConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitDecimalConstant(@Nonnull RelationalParser.DecimalConstantContext ctx) {
        return getDelegate().visitDecimalConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNegativeDecimalConstant(@Nonnull RelationalParser.NegativeDecimalConstantContext ctx) {
        return getDelegate().visitNegativeDecimalConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBytesConstant(@Nonnull RelationalParser.BytesConstantContext ctx) {
        return getDelegate().visitBytesConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBooleanConstant(@Nonnull RelationalParser.BooleanConstantContext ctx) {
        return getDelegate().visitBooleanConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBitStringConstant(@Nonnull RelationalParser.BitStringConstantContext ctx) {
        return getDelegate().visitBitStringConstant(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNullConstant(@Nonnull RelationalParser.NullConstantContext ctx) {
        return getDelegate().visitNullConstant(ctx);
    }

    @Override
    @Nonnull
    public Object visitStringDataType(@Nonnull RelationalParser.StringDataTypeContext ctx) {
        return getDelegate().visitStringDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitNationalStringDataType(@Nonnull RelationalParser.NationalStringDataTypeContext ctx) {
        return getDelegate().visitNationalStringDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitNationalVaryingStringDataType(@Nonnull RelationalParser.NationalVaryingStringDataTypeContext ctx) {
        return getDelegate().visitNationalVaryingStringDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitDimensionDataType(@Nonnull RelationalParser.DimensionDataTypeContext ctx) {
        return getDelegate().visitDimensionDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitSimpleDataType(@Nonnull RelationalParser.SimpleDataTypeContext ctx) {
        return getDelegate().visitSimpleDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitCollectionDataType(@Nonnull RelationalParser.CollectionDataTypeContext ctx) {
        return getDelegate().visitCollectionDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitSpatialDataType(@Nonnull RelationalParser.SpatialDataTypeContext ctx) {
        return getDelegate().visitSpatialDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitLongVarcharDataType(@Nonnull RelationalParser.LongVarcharDataTypeContext ctx) {
        return getDelegate().visitLongVarcharDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitLongVarbinaryDataType(@Nonnull RelationalParser.LongVarbinaryDataTypeContext ctx) {
        return getDelegate().visitLongVarbinaryDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitCollectionOptions(@Nonnull RelationalParser.CollectionOptionsContext ctx) {
        return getDelegate().visitCollectionOptions(ctx);
    }

    @Override
    @Nonnull
    public Object visitConvertedDataType(@Nonnull RelationalParser.ConvertedDataTypeContext ctx) {
        return getDelegate().visitConvertedDataType(ctx);
    }

    @Override
    @Nonnull
    public Object visitLengthOneDimension(@Nonnull RelationalParser.LengthOneDimensionContext ctx) {
        return getDelegate().visitLengthOneDimension(ctx);
    }

    @Override
    @Nonnull
    public Object visitLengthTwoDimension(@Nonnull RelationalParser.LengthTwoDimensionContext ctx) {
        return getDelegate().visitLengthTwoDimension(ctx);
    }

    @Override
    @Nonnull
    public Object visitLengthTwoOptionalDimension(@Nonnull RelationalParser.LengthTwoOptionalDimensionContext ctx) {
        return getDelegate().visitLengthTwoOptionalDimension(ctx);
    }

    @Override
    @Nonnull
    public List<Identifier> visitUidList(@Nonnull RelationalParser.UidListContext ctx) {
        return getDelegate().visitUidList(ctx);
    }

    @Override
    @Nonnull
    public Pair<String, StringTrieNode> visitUidWithNestings(@Nonnull RelationalParser.UidWithNestingsContext ctx) {
        return getDelegate().visitUidWithNestings(ctx);
    }

    @Override
    @Nonnull
    public StringTrieNode visitUidListWithNestingsInParens(@Nonnull RelationalParser.UidListWithNestingsInParensContext ctx) {
        return getDelegate().visitUidListWithNestingsInParens(ctx);
    }

    @Override
    @Nonnull
    public StringTrieNode visitUidListWithNestings(@Nonnull RelationalParser.UidListWithNestingsContext ctx) {
        return getDelegate().visitUidListWithNestings(ctx);
    }

    @Override
    @Nonnull
    public Object visitTables(@Nonnull RelationalParser.TablesContext ctx) {
        return getDelegate().visitTables(ctx);
    }

    @Override
    @Nonnull
    public Object visitIndexColumnNames(@Nonnull RelationalParser.IndexColumnNamesContext ctx) {
        return getDelegate().visitIndexColumnNames(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitExpressions(@Nonnull RelationalParser.ExpressionsContext ctx) {
        return getDelegate().visitExpressions(ctx);
    }

    @Override
    @Nonnull
    public Object visitExpressionsWithDefaults(@Nonnull RelationalParser.ExpressionsWithDefaultsContext ctx) {
        return getDelegate().visitExpressionsWithDefaults(ctx);
    }

    @Override
    @Nonnull
    public Expression visitRecordConstructorForInsert(@Nonnull RelationalParser.RecordConstructorForInsertContext ctx) {
        return getDelegate().visitRecordConstructorForInsert(ctx);
    }

    @Override
    @Nonnull
    public Expression visitRecordConstructor(@Nonnull RelationalParser.RecordConstructorContext ctx) {
        return getDelegate().visitRecordConstructor(ctx);
    }

    @Override
    @Nonnull
    public Object visitOfTypeClause(@Nonnull RelationalParser.OfTypeClauseContext ctx) {
        return getDelegate().visitOfTypeClause(ctx);
    }

    @Override
    @Nonnull
    public Expression visitArrayConstructor(@Nonnull RelationalParser.ArrayConstructorContext ctx) {
        return getDelegate().visitArrayConstructor(ctx);
    }

    @Override
    @Nonnull
    public Object visitUserVariables(@Nonnull RelationalParser.UserVariablesContext ctx) {
        return getDelegate().visitUserVariables(ctx);
    }

    @Override
    @Nonnull
    public Object visitDefaultValue(@Nonnull RelationalParser.DefaultValueContext ctx) {
        return getDelegate().visitDefaultValue(ctx);
    }

    @Override
    @Nonnull
    public Object visitCurrentTimestamp(@Nonnull RelationalParser.CurrentTimestampContext ctx) {
        return getDelegate().visitCurrentTimestamp(ctx);
    }

    @Override
    @Nonnull
    public Object visitExpressionOrDefault(@Nonnull RelationalParser.ExpressionOrDefaultContext ctx) {
        return getDelegate().visitExpressionOrDefault(ctx);
    }

    @Override
    @Nonnull
    public Expression visitExpressionWithName(@Nonnull RelationalParser.ExpressionWithNameContext ctx) {
        return getDelegate().visitExpressionWithName(ctx);
    }

    @Override
    @Nonnull
    public Expression visitExpressionWithOptionalName(@Nonnull RelationalParser.ExpressionWithOptionalNameContext ctx) {
        return getDelegate().visitExpressionWithOptionalName(ctx);
    }

    @Override
    @Nonnull
    public Object visitIfExists(@Nonnull RelationalParser.IfExistsContext ctx) {
        return getDelegate().visitIfExists(ctx);
    }

    @Override
    @Nonnull
    public Object visitIfNotExists(@Nonnull RelationalParser.IfNotExistsContext ctx) {
        return getDelegate().visitIfNotExists(ctx);
    }

    @Override
    @Nonnull
    public Expression visitAggregateFunctionCall(@Nonnull RelationalParser.AggregateFunctionCallContext ctx) {
        return getDelegate().visitAggregateFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitSpecificFunctionCall(@Nonnull RelationalParser.SpecificFunctionCallContext ctx) {
        return getDelegate().visitSpecificFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Expression visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx) {
        return getDelegate().visitScalarFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitSimpleFunctionCall(@Nonnull RelationalParser.SimpleFunctionCallContext ctx) {
        return getDelegate().visitSimpleFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitDataTypeFunctionCall(@Nonnull RelationalParser.DataTypeFunctionCallContext ctx) {
        return getDelegate().visitDataTypeFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitValuesFunctionCall(@Nonnull RelationalParser.ValuesFunctionCallContext ctx) {
        return getDelegate().visitValuesFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitCaseExpressionFunctionCall(@Nonnull RelationalParser.CaseExpressionFunctionCallContext ctx) {
        return getDelegate().visitCaseExpressionFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Expression visitCaseFunctionCall(@Nonnull RelationalParser.CaseFunctionCallContext ctx) {
        return getDelegate().visitCaseFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitCharFunctionCall(@Nonnull RelationalParser.CharFunctionCallContext ctx) {
        return getDelegate().visitCharFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitPositionFunctionCall(@Nonnull RelationalParser.PositionFunctionCallContext ctx) {
        return getDelegate().visitPositionFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitSubstrFunctionCall(@Nonnull RelationalParser.SubstrFunctionCallContext ctx) {
        return getDelegate().visitSubstrFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitTrimFunctionCall(@Nonnull RelationalParser.TrimFunctionCallContext ctx) {
        return getDelegate().visitTrimFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitWeightFunctionCall(@Nonnull RelationalParser.WeightFunctionCallContext ctx) {
        return getDelegate().visitWeightFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitExtractFunctionCall(@Nonnull RelationalParser.ExtractFunctionCallContext ctx) {
        return getDelegate().visitExtractFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitGetFormatFunctionCall(@Nonnull RelationalParser.GetFormatFunctionCallContext ctx) {
        return getDelegate().visitGetFormatFunctionCall(ctx);
    }

    @Override
    @Nonnull
    public Object visitCaseFuncAlternative(@Nonnull RelationalParser.CaseFuncAlternativeContext ctx) {
        return getDelegate().visitCaseFuncAlternative(ctx);
    }

    @Override
    @Nonnull
    public Object visitLevelWeightList(@Nonnull RelationalParser.LevelWeightListContext ctx) {
        return getDelegate().visitLevelWeightList(ctx);
    }

    @Override
    @Nonnull
    public Object visitLevelWeightRange(@Nonnull RelationalParser.LevelWeightRangeContext ctx) {
        return getDelegate().visitLevelWeightRange(ctx);
    }

    @Override
    @Nonnull
    public Object visitLevelInWeightListElement(@Nonnull RelationalParser.LevelInWeightListElementContext ctx) {
        return getDelegate().visitLevelInWeightListElement(ctx);
    }

    @Override
    @Nonnull
    public Expression visitAggregateWindowedFunction(@Nonnull RelationalParser.AggregateWindowedFunctionContext ctx) {
        return getDelegate().visitAggregateWindowedFunction(ctx);
    }

    @Override
    @Nonnull
    public Object visitNonAggregateWindowedFunction(@Nonnull RelationalParser.NonAggregateWindowedFunctionContext ctx) {
        return getDelegate().visitNonAggregateWindowedFunction(ctx);
    }

    @Override
    @Nonnull
    public Object visitOverClause(@Nonnull RelationalParser.OverClauseContext ctx) {
        return getDelegate().visitOverClause(ctx);
    }

    @Override
    @Nonnull
    public Object visitWindowName(@Nonnull RelationalParser.WindowNameContext ctx) {
        return getDelegate().visitWindowName(ctx);
    }

    @Override
    @Nonnull
    public Object visitScalarFunctionName(@Nonnull RelationalParser.ScalarFunctionNameContext ctx) {
        return getDelegate().visitScalarFunctionName(ctx);
    }

    @Override
    @Nonnull
    public Expressions visitFunctionArgs(@Nonnull RelationalParser.FunctionArgsContext ctx) {
        return getDelegate().visitFunctionArgs(ctx);
    }

    @Override
    @Nonnull
    public Object visitFunctionArg(@Nonnull RelationalParser.FunctionArgContext ctx) {
        return getDelegate().visitFunctionArg(ctx);
    }

    @Override
    @Nonnull
    public Expression visitIsExpression(@Nonnull RelationalParser.IsExpressionContext ctx) {
        return getDelegate().visitIsExpression(ctx);
    }

    @Override
    @Nonnull
    public Expression visitNotExpression(@Nonnull RelationalParser.NotExpressionContext ctx) {
        return getDelegate().visitNotExpression(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLikePredicate(@Nonnull RelationalParser.LikePredicateContext ctx) {
        return getDelegate().visitLikePredicate(ctx);
    }

    @Override
    @Nonnull
    public Expression visitLogicalExpression(@Nonnull RelationalParser.LogicalExpressionContext ctx) {
        return getDelegate().visitLogicalExpression(ctx);
    }

    @Override
    @Nonnull
    public Object visitPredicateExpression(@Nonnull RelationalParser.PredicateExpressionContext ctx) {
        return getDelegate().visitPredicateExpression(ctx);
    }

    @Override
    @Nonnull
    public Object visitExpressionAtomPredicate(@Nonnull RelationalParser.ExpressionAtomPredicateContext ctx) {
        return getDelegate().visitExpressionAtomPredicate(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBinaryComparisonPredicate(@Nonnull RelationalParser.BinaryComparisonPredicateContext ctx) {
        return getDelegate().visitBinaryComparisonPredicate(ctx);
    }

    @Override
    @Nonnull
    public Expression visitInPredicate(@Nonnull RelationalParser.InPredicateContext ctx) {
        return getDelegate().visitInPredicate(ctx);
    }

    @Override
    @Nonnull
    public Expression visitInList(@Nonnull RelationalParser.InListContext ctx) {
        return getDelegate().visitInList(ctx);
    }

    @Override
    @Nonnull
    public Object visitJsonExpressionAtom(@Nonnull RelationalParser.JsonExpressionAtomContext ctx) {
        return getDelegate().visitJsonExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitSubqueryExpressionAtom(@Nonnull RelationalParser.SubqueryExpressionAtomContext ctx) {
        return getDelegate().visitSubqueryExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitConstantExpressionAtom(@Nonnull RelationalParser.ConstantExpressionAtomContext ctx) {
        return getDelegate().visitConstantExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitFunctionCallExpressionAtom(@Nonnull RelationalParser.FunctionCallExpressionAtomContext ctx) {
        return getDelegate().visitFunctionCallExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitFullColumnNameExpressionAtom(@Nonnull RelationalParser.FullColumnNameExpressionAtomContext ctx) {
        return getDelegate().visitFullColumnNameExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitBitExpressionAtom(@Nonnull RelationalParser.BitExpressionAtomContext ctx) {
        return getDelegate().visitBitExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitPreparedStatementParameterAtom(@Nonnull RelationalParser.PreparedStatementParameterAtomContext ctx) {
        return getDelegate().visitPreparedStatementParameterAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitRecordConstructorExpressionAtom(@Nonnull RelationalParser.RecordConstructorExpressionAtomContext ctx) {
        return getDelegate().visitRecordConstructorExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitArrayConstructorExpressionAtom(@Nonnull RelationalParser.ArrayConstructorExpressionAtomContext ctx) {
        return getDelegate().visitArrayConstructorExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitMathExpressionAtom(@Nonnull RelationalParser.MathExpressionAtomContext ctx) {
        return getDelegate().visitMathExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitExistsExpressionAtom(@Nonnull RelationalParser.ExistsExpressionAtomContext ctx) {
        return getDelegate().visitExistsExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Object visitIntervalExpressionAtom(@Nonnull RelationalParser.IntervalExpressionAtomContext ctx) {
        return getDelegate().visitIntervalExpressionAtom(ctx);
    }

    @Override
    @Nonnull
    public Expression visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx) {
        return getDelegate().visitPreparedStatementParameter(ctx);
    }

    @Override
    @Nonnull
    public Object visitUnaryOperator(@Nonnull RelationalParser.UnaryOperatorContext ctx) {
        return getDelegate().visitUnaryOperator(ctx);
    }

    @Override
    @Nonnull
    public Object visitComparisonOperator(@Nonnull RelationalParser.ComparisonOperatorContext ctx) {
        return getDelegate().visitComparisonOperator(ctx);
    }

    @Override
    @Nonnull
    public Object visitLogicalOperator(@Nonnull RelationalParser.LogicalOperatorContext ctx) {
        return getDelegate().visitLogicalOperator(ctx);
    }

    @Override
    @Nonnull
    public Object visitBitOperator(@Nonnull RelationalParser.BitOperatorContext ctx) {
        return getDelegate().visitBitOperator(ctx);
    }

    @Override
    @Nonnull
    public Object visitMathOperator(@Nonnull RelationalParser.MathOperatorContext ctx) {
        return getDelegate().visitMathOperator(ctx);
    }

    @Override
    @Nonnull
    public Object visitJsonOperator(@Nonnull RelationalParser.JsonOperatorContext ctx) {
        return getDelegate().visitJsonOperator(ctx);
    }

    @Override
    @Nonnull
    public Object visitCharsetNameBase(@Nonnull RelationalParser.CharsetNameBaseContext ctx) {
        return getDelegate().visitCharsetNameBase(ctx);
    }

    @Override
    @Nonnull
    public Object visitIntervalTypeBase(@Nonnull RelationalParser.IntervalTypeBaseContext ctx) {
        return getDelegate().visitIntervalTypeBase(ctx);
    }

    @Override
    @Nonnull
    public Object visitKeywordsCanBeId(@Nonnull RelationalParser.KeywordsCanBeIdContext ctx) {
        return getDelegate().visitKeywordsCanBeId(ctx);
    }

    @Override
    @Nonnull
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
