/*
 * TypedVisitor.java
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

import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserVisitor;
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
import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Set;

/**
 * This interface specializes the signatures of the auto-generated {@link RelationalParserVisitor} to make
 * it type-safe and make it less error-prune.
 * <br>
 * <b>Note</b> When extending {@link RelationalParser} you should override the generated visitation method with a
 * proper return type, and provide an implementation of it in the corresponding specialized visitor.
 * See {@link DdlVisitor}, {@link QueryVisitor}, {@link IdentifierVisitor}, {@link MetadataPlanVisitor}, and
 * {@link ExpressionVisitor} for more information.
 */
public interface TypedVisitor extends RelationalParserVisitor<Object> {

    @Override
    Object visitRoot(RelationalParser.RootContext ctx);

    @Override
    Object visitStatements(RelationalParser.StatementsContext ctx);

    @Nullable
    @Override
    Object visitStatement(RelationalParser.StatementContext ctx);

    @Override
    QueryPlan.LogicalQueryPlan visitDmlStatement(RelationalParser.DmlStatementContext ctx);

    @Override
    Object visitDdlStatement(RelationalParser.DdlStatementContext ctx);

    @Override
    Object visitTransactionStatement(RelationalParser.TransactionStatementContext ctx);

    @Override
    Object visitPreparedStatement(RelationalParser.PreparedStatementContext ctx);

    @Override
    Object visitAdministrationStatement(RelationalParser.AdministrationStatementContext ctx);

    @Override
    Object visitUtilityStatement(RelationalParser.UtilityStatementContext ctx);

    @Override
    Object visitTemplateClause(RelationalParser.TemplateClauseContext ctx);

    @Override
    ProceduralPlan visitCreateSchemaStatement(RelationalParser.CreateSchemaStatementContext ctx);

    @Override
    ProceduralPlan visitCreateSchemaTemplateStatement(RelationalParser.CreateSchemaTemplateStatementContext ctx);

    @Override
    ProceduralPlan visitCreateDatabaseStatement(RelationalParser.CreateDatabaseStatementContext ctx);

    @Override
    Object visitOptionsClause(RelationalParser.OptionsClauseContext ctx);

    @Override
    Object visitOption(RelationalParser.OptionContext ctx);

    @Override
    ProceduralPlan visitDropDatabaseStatement(RelationalParser.DropDatabaseStatementContext ctx);

    @Override
    ProceduralPlan visitDropSchemaTemplateStatement(RelationalParser.DropSchemaTemplateStatementContext ctx);

    @Override
    ProceduralPlan visitDropSchemaStatement(RelationalParser.DropSchemaStatementContext ctx);

    @Override
    RecordLayerTable visitStructDefinition(RelationalParser.StructDefinitionContext ctx);

    @Override
    RecordLayerTable visitTableDefinition(RelationalParser.TableDefinitionContext ctx);

    @Override
    Object visitColumnDefinition(RelationalParser.ColumnDefinitionContext ctx);

    @Override
    DataType visitFunctionColumnType(RelationalParser.FunctionColumnTypeContext ctx);

    @Override
    Object visitColumnType(RelationalParser.ColumnTypeContext ctx);

    @Override
    Boolean visitNullColumnConstraint(RelationalParser.NullColumnConstraintContext ctx);

    @Override
    Object visitPrimaryKeyDefinition(RelationalParser.PrimaryKeyDefinitionContext ctx);

    @Override
    List<Identifier> visitFullIdList(RelationalParser.FullIdListContext ctx);

    @Override
    DataType.Named visitEnumDefinition(RelationalParser.EnumDefinitionContext ctx);

    @Override
    RecordLayerIndex visitIndexAsSelectDefinition(RelationalParser.IndexAsSelectDefinitionContext ctx);

    @Override
    RecordLayerIndex visitIndexOnSourceDefinition(RelationalParser.IndexOnSourceDefinitionContext ctx);

    @Override
    RecordLayerIndex visitVectorIndexDefinition(RelationalParser.VectorIndexDefinitionContext ctx);

    @Override
    Object visitIndexColumnList(RelationalParser.IndexColumnListContext ctx);

    @Override
    Object visitIndexColumnSpec(RelationalParser.IndexColumnSpecContext ctx);

    @Override
    Object visitIncludeClause(RelationalParser.IncludeClauseContext ctx);

    @Override
    Object visitIndexAttributes(RelationalParser.IndexAttributesContext ctx);

    @Override
    Object visitIndexAttribute(RelationalParser.IndexAttributeContext ctx);

    @Override
    ProceduralPlan visitCreateTempFunction(RelationalParser.CreateTempFunctionContext ctx);

    @Override
    ProceduralPlan visitDropTempFunction(RelationalParser.DropTempFunctionContext ctx);

    @Override
    CompiledSqlFunction visitTempSqlInvokedFunction(RelationalParser.TempSqlInvokedFunctionContext ctx);

    @Override
    UserDefinedFunction visitSqlInvokedFunction(RelationalParser.SqlInvokedFunctionContext ctx);

    @Override
    Expression visitUserDefinedMacroFunctionStatementBody(RelationalParser.UserDefinedMacroFunctionStatementBodyContext ctx);

    @Override
    LogicalOperator visitStatementBody(RelationalParser.StatementBodyContext ctx);

    @Override
    RecordLayerInvokedRoutine visitFunctionSpecification(RelationalParser.FunctionSpecificationContext ctx);

    @Override
    Expressions visitSqlParameterDeclarationList(RelationalParser.SqlParameterDeclarationListContext ctx);

    @Override
    Expressions visitSqlParameterDeclarations(RelationalParser.SqlParameterDeclarationsContext ctx);

    @Override
    Expression visitSqlParameterDeclaration(RelationalParser.SqlParameterDeclarationContext ctx);

    @Override
    DataType visitReturnsType(RelationalParser.ReturnsTypeContext ctx);

    @Override
    Object visitCharSet(RelationalParser.CharSetContext ctx);

    @Override
    Object visitIntervalType(RelationalParser.IntervalTypeContext ctx);

    @Override
    Object visitSchemaId(RelationalParser.SchemaIdContext ctx);

    @Override
    Object visitPath(RelationalParser.PathContext ctx);

    @Override
    Object visitSchemaTemplateId(RelationalParser.SchemaTemplateIdContext ctx);

    @Override
    LogicalOperator visitDeleteStatement(RelationalParser.DeleteStatementContext ctx);

    @Override
    LogicalOperator visitInsertStatement(RelationalParser.InsertStatementContext ctx);

    @Override
    QueryPlan.LogicalQueryPlan visitSelectStatement(RelationalParser.SelectStatementContext ctx);

    @Override
    LogicalOperator visitQuery(RelationalParser.QueryContext ctx);

    @Nullable
    @Override
    Void visitCtes(RelationalParser.CtesContext ctx);

    @Override
    LogicalOperator visitNamedQuery(RelationalParser.NamedQueryContext ctx);

    @Override
    LogicalOperator visitTableFunction(RelationalParser.TableFunctionContext ctx);

    @Override
    Expressions visitNamedOrUnnamedFunctionArgs(RelationalParser.NamedOrUnnamedFunctionArgsContext ctx);

    @Override
    Identifier visitTableFunctionName(RelationalParser.TableFunctionNameContext ctx);

    @Override
    Expression visitContinuationAtom(RelationalParser.ContinuationAtomContext ctx);

    @Override
    LogicalOperator visitQueryTermDefault(RelationalParser.QueryTermDefaultContext ctx);

    @Override
    LogicalOperator visitSetQuery(RelationalParser.SetQueryContext ctx);

    @Override
    LogicalOperator visitInsertStatementValueSelect(RelationalParser.InsertStatementValueSelectContext ctx);

    @Override
    LogicalOperator visitInsertStatementValueValues(RelationalParser.InsertStatementValueValuesContext ctx);

    @Override
    Expressions visitUpdatedElement(RelationalParser.UpdatedElementContext ctx);

    @Override
    Object visitAssignmentField(RelationalParser.AssignmentFieldContext ctx);

    @Override
    LogicalOperator visitUpdateStatement(RelationalParser.UpdateStatementContext ctx);

    @Override
    List<OrderByExpression> visitOrderByClause(RelationalParser.OrderByClauseContext ctx);

    @Override
    OrderByExpression visitOrderByExpression(RelationalParser.OrderByExpressionContext ctx);

    @Override
    @Nullable
    Void visitTableSources(RelationalParser.TableSourcesContext ctx);

    @Nullable
    @Override
    Void visitTableSourceBase(RelationalParser.TableSourceBaseContext ctx);

    @Override
    LogicalOperator visitAtomTableItem(RelationalParser.AtomTableItemContext ctx);

    @Override
    LogicalOperator visitSubqueryTableItem(RelationalParser.SubqueryTableItemContext ctx);

    @Override
    LogicalOperator visitInlineTableItem(RelationalParser.InlineTableItemContext ctx);

    @Override
    LogicalOperator visitTableValuedFunction(RelationalParser.TableValuedFunctionContext ctx);

    @Override
    Set<String> visitIndexHint(RelationalParser.IndexHintContext ctx);

    @Override
    Object visitIndexHintType(RelationalParser.IndexHintTypeContext ctx);

    @Override
    NonnullPair<String, CompatibleTypeEvolutionPredicate.FieldAccessTrieNode> visitInlineTableDefinition(RelationalParser.InlineTableDefinitionContext ctx);

    @Nullable
    @Override
    Object visitInnerJoin(RelationalParser.InnerJoinContext ctx);

    @Override
    Object visitStraightJoin(RelationalParser.StraightJoinContext ctx);

    @Nullable
    @Override
    Object visitOuterJoin(RelationalParser.OuterJoinContext ctx);

    @Override
    Object visitNaturalJoin(RelationalParser.NaturalJoinContext ctx);

    @Override
    LogicalOperator visitSimpleTable(RelationalParser.SimpleTableContext ctx);

    @Override
    LogicalOperator visitParenthesisQuery(RelationalParser.ParenthesisQueryContext ctx);

    @Override
    Expressions visitSelectElements(RelationalParser.SelectElementsContext ctx);

    @Override
    Expression visitSelectStarElement(RelationalParser.SelectStarElementContext ctx);

    @Override
    Object visitSelectQualifierStarElement(RelationalParser.SelectQualifierStarElementContext ctx);

    @Override
    Expression visitSelectExpressionElement(RelationalParser.SelectExpressionElementContext ctx);

    @Override
    @Nullable
    Void visitFromClause(RelationalParser.FromClauseContext ctx);

    @Override
    Expressions visitGroupByClause(RelationalParser.GroupByClauseContext ctx);

    @Override
    Expression visitWhereExpr(RelationalParser.WhereExprContext ctx);

    @Override
    Expression visitHavingClause(RelationalParser.HavingClauseContext ctx);

    @Override
    Expression visitQualifyClause(RelationalParser.QualifyClauseContext ctx);

    @Override
    Expression visitGroupByItem(RelationalParser.GroupByItemContext ctx);

    @Override
    Expression visitLimitClause(RelationalParser.LimitClauseContext ctx);

    @Override
    Expression visitLimitClauseAtom(RelationalParser.LimitClauseAtomContext ctx);

    @Override
    Object visitStatementOptions(RelationalParser.StatementOptionsContext ctx);

    @Override
    Object visitStatementOption(RelationalParser.StatementOptionContext ctx);

    @Override
    Object visitStartTransaction(RelationalParser.StartTransactionContext ctx);

    @Override
    Object visitCommitStatement(RelationalParser.CommitStatementContext ctx);

    @Override
    Object visitRollbackStatement(RelationalParser.RollbackStatementContext ctx);

    @Override
    Object visitSetAutocommitStatement(RelationalParser.SetAutocommitStatementContext ctx);

    @Override
    Object visitSetTransactionStatement(RelationalParser.SetTransactionStatementContext ctx);

    @Override
    Object visitTransactionOption(RelationalParser.TransactionOptionContext ctx);

    @Override
    Object visitTransactionLevel(RelationalParser.TransactionLevelContext ctx);

    @Override
    Object visitPrepareStatement(RelationalParser.PrepareStatementContext ctx);

    @Override
    Object visitExecuteStatement(RelationalParser.ExecuteStatementContext ctx);

    @Override
    QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(RelationalParser.ShowDatabasesStatementContext ctx);

    @Override
    QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(RelationalParser.ShowSchemaTemplatesStatementContext ctx);

    @Override
    Object visitSetVariable(RelationalParser.SetVariableContext ctx);

    @Override
    Object visitSetCharset(RelationalParser.SetCharsetContext ctx);

    @Override
    Object visitSetNames(RelationalParser.SetNamesContext ctx);

    @Override
    Object visitSetTransaction(RelationalParser.SetTransactionContext ctx);

    @Override
    Object visitSetAutocommit(RelationalParser.SetAutocommitContext ctx);

    @Override
    Object visitSetNewValueInsideTrigger(RelationalParser.SetNewValueInsideTriggerContext ctx);

    @Override
    Object visitVariableClause(RelationalParser.VariableClauseContext ctx);

    @Override
    Object visitKillStatement(RelationalParser.KillStatementContext ctx);

    @Override
    Object visitResetStatement(RelationalParser.ResetStatementContext ctx);

    @Override
    Object visitTableIndexes(RelationalParser.TableIndexesContext ctx);

    @Override
    Object visitLoadedTableIndexes(RelationalParser.LoadedTableIndexesContext ctx);

    @Override
    QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(RelationalParser.SimpleDescribeSchemaStatementContext ctx);

    @Override
    QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx);

    @Override
    QueryPlan.LogicalQueryPlan visitFullDescribeStatement(RelationalParser.FullDescribeStatementContext ctx);

    @Override
    Object visitHelpStatement(RelationalParser.HelpStatementContext ctx);

    @Override
    Object visitDescribeStatements(RelationalParser.DescribeStatementsContext ctx);

    @Override
    Object visitDescribeConnection(RelationalParser.DescribeConnectionContext ctx);

    @Override
    Identifier visitFullId(RelationalParser.FullIdContext ctx);

    @Override
    Identifier visitTableName(RelationalParser.TableNameContext ctx);

    @Override
    Expression visitFullColumnName(RelationalParser.FullColumnNameContext ctx);

    @Override
    Identifier visitIndexColumnName(RelationalParser.IndexColumnNameContext ctx);

    @Override
    Identifier visitCharsetName(RelationalParser.CharsetNameContext ctx);

    @Override
    Identifier visitCollationName(RelationalParser.CollationNameContext ctx);

    @Override
    Identifier visitUid(RelationalParser.UidContext ctx);

    @Override
    Identifier visitSimpleId(RelationalParser.SimpleIdContext ctx);

    @Override
    Object visitNullNotnull(RelationalParser.NullNotnullContext ctx);

    @Override
    Expression visitDecimalLiteral(RelationalParser.DecimalLiteralContext ctx);

    @Override
    Expression visitStringLiteral(RelationalParser.StringLiteralContext ctx);

    @Override
    Expression visitBooleanLiteral(RelationalParser.BooleanLiteralContext ctx);

    @Override
    Expression visitBytesLiteral(RelationalParser.BytesLiteralContext ctx);

    @Override
    Expression visitNullLiteral(RelationalParser.NullLiteralContext ctx);

    @Override
    Expression visitStringConstant(RelationalParser.StringConstantContext ctx);

    @Override
    Expression visitDecimalConstant(RelationalParser.DecimalConstantContext ctx);

    @Override
    Expression visitNegativeDecimalConstant(RelationalParser.NegativeDecimalConstantContext ctx);

    @Override
    Expression visitBytesConstant(RelationalParser.BytesConstantContext ctx);

    @Override
    Expression visitBooleanConstant(RelationalParser.BooleanConstantContext ctx);

    @Override
    Expression visitBitStringConstant(RelationalParser.BitStringConstantContext ctx);

    @Override
    Expression visitNullConstant(RelationalParser.NullConstantContext ctx);

    @Override
    Object visitStringDataType(RelationalParser.StringDataTypeContext ctx);

    @Override
    Object visitNationalStringDataType(RelationalParser.NationalStringDataTypeContext ctx);

    @Override
    Object visitNationalVaryingStringDataType(RelationalParser.NationalVaryingStringDataTypeContext ctx);

    @Override
    Object visitDimensionDataType(RelationalParser.DimensionDataTypeContext ctx);

    @Override
    Object visitSimpleDataType(RelationalParser.SimpleDataTypeContext ctx);

    @Override
    Object visitCollectionDataType(RelationalParser.CollectionDataTypeContext ctx);

    @Override
    Object visitSpatialDataType(RelationalParser.SpatialDataTypeContext ctx);

    @Override
    Object visitLongVarcharDataType(RelationalParser.LongVarcharDataTypeContext ctx);

    @Override
    Object visitLongVarbinaryDataType(RelationalParser.LongVarbinaryDataTypeContext ctx);

    @Override
    Object visitCollectionOptions(RelationalParser.CollectionOptionsContext ctx);

    @Override
    Object visitConvertedDataType(RelationalParser.ConvertedDataTypeContext ctx);

    @Override
    Object visitLengthOneDimension(RelationalParser.LengthOneDimensionContext ctx);

    @Override
    Object visitLengthTwoDimension(RelationalParser.LengthTwoDimensionContext ctx);

    @Override
    Object visitLengthTwoOptionalDimension(RelationalParser.LengthTwoOptionalDimensionContext ctx);

    @Override
    List<Identifier> visitUidList(RelationalParser.UidListContext ctx);

    @Override
    Object visitUidWithNestings(RelationalParser.UidWithNestingsContext ctx);

    @Override
    CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestingsInParens(RelationalParser.UidListWithNestingsInParensContext ctx);

    @Override
    CompatibleTypeEvolutionPredicate.FieldAccessTrieNode visitUidListWithNestings(RelationalParser.UidListWithNestingsContext ctx);

    @Override
    Object visitTables(RelationalParser.TablesContext ctx);

    @Override
    Object visitIndexColumnNames(RelationalParser.IndexColumnNamesContext ctx);

    @Override
    Expressions visitExpressions(RelationalParser.ExpressionsContext ctx);

    @Override
    Object visitExpressionsWithDefaults(RelationalParser.ExpressionsWithDefaultsContext ctx);

    @Override
    Expression visitRecordConstructorForInsert(RelationalParser.RecordConstructorForInsertContext ctx);

    @Override
    Expression visitRecordConstructorForInlineTable(RelationalParser.RecordConstructorForInlineTableContext ctx);

    @Override
    Expression visitRecordConstructor(RelationalParser.RecordConstructorContext ctx);

    @Override
    Object visitOfTypeClause(RelationalParser.OfTypeClauseContext ctx);

    @Override
    Expression visitArrayConstructor(RelationalParser.ArrayConstructorContext ctx);

    @Override
    Object visitUserVariables(RelationalParser.UserVariablesContext ctx);

    @Override
    Object visitDefaultValue(RelationalParser.DefaultValueContext ctx);

    @Override
    Object visitCurrentTimestamp(RelationalParser.CurrentTimestampContext ctx);

    @Override
    Object visitExpressionOrDefault(RelationalParser.ExpressionOrDefaultContext ctx);

    @Override
    Expression visitExpressionWithOptionalName(RelationalParser.ExpressionWithOptionalNameContext ctx);

    @Nullable
    @Override
    Object visitIfExists(RelationalParser.IfExistsContext ctx);

    @Nullable
    @Override
    Object visitIfNotExists(RelationalParser.IfNotExistsContext ctx);

    @Override
    Expression visitAggregateFunctionCall(RelationalParser.AggregateFunctionCallContext ctx);

    @Override
    Expression visitNonAggregateFunctionCall(RelationalParser.NonAggregateFunctionCallContext ctx);

    @Override
    Expression visitUserDefinedScalarFunctionCall(RelationalParser.UserDefinedScalarFunctionCallContext ctx);

    @Override
    Object visitSpecificFunctionCall(RelationalParser.SpecificFunctionCallContext ctx);

    @Override
    Expression visitScalarFunctionCall(RelationalParser.ScalarFunctionCallContext ctx);

    @Override
    Object visitSimpleFunctionCall(RelationalParser.SimpleFunctionCallContext ctx);

    @Override
    Object visitDataTypeFunctionCall(RelationalParser.DataTypeFunctionCallContext ctx);

    @Override
    Object visitValuesFunctionCall(RelationalParser.ValuesFunctionCallContext ctx);

    @Override
    Object visitCaseExpressionFunctionCall(RelationalParser.CaseExpressionFunctionCallContext ctx);

    @Override
    Expression visitCaseFunctionCall(RelationalParser.CaseFunctionCallContext ctx);

    @Override
    Object visitCharFunctionCall(RelationalParser.CharFunctionCallContext ctx);

    @Override
    Object visitPositionFunctionCall(RelationalParser.PositionFunctionCallContext ctx);

    @Override
    Object visitSubstrFunctionCall(RelationalParser.SubstrFunctionCallContext ctx);

    @Override
    Object visitTrimFunctionCall(RelationalParser.TrimFunctionCallContext ctx);

    @Override
    Object visitWeightFunctionCall(RelationalParser.WeightFunctionCallContext ctx);

    @Override
    Object visitExtractFunctionCall(RelationalParser.ExtractFunctionCallContext ctx);

    @Override
    Object visitGetFormatFunctionCall(RelationalParser.GetFormatFunctionCallContext ctx);

    @Override
    Object visitCaseFuncAlternative(RelationalParser.CaseFuncAlternativeContext ctx);

    @Override
    Object visitLevelWeightList(RelationalParser.LevelWeightListContext ctx);

    @Override
    Object visitLevelWeightRange(RelationalParser.LevelWeightRangeContext ctx);

    @Override
    Object visitLevelInWeightListElement(RelationalParser.LevelInWeightListElementContext ctx);

    @Override
    Expression visitAggregateWindowedFunction(RelationalParser.AggregateWindowedFunctionContext ctx);

    @Override
    Boolean visitNullTreatmentClause(RelationalParser.NullTreatmentClauseContext ctx);

    @Override
    Expression visitNonAggregateWindowedFunction(RelationalParser.NonAggregateWindowedFunctionContext ctx);

    @Override
    WindowSpecExpression visitOverClause(RelationalParser.OverClauseContext ctx);

    @Override
    Expressions visitPartitionClause(RelationalParser.PartitionClauseContext ctx);

    @Override
    Object visitWindowName(RelationalParser.WindowNameContext ctx);

    @Override
    Expressions visitWindowOptionsClause(RelationalParser.WindowOptionsClauseContext ctx);

    @Override
    Expression visitWindowOption(RelationalParser.WindowOptionContext ctx);

    @Override
    Object visitScalarFunctionName(RelationalParser.ScalarFunctionNameContext ctx);

    @Override
    Expressions visitFunctionArgs(RelationalParser.FunctionArgsContext ctx);

    @Override
    Object visitFunctionArg(RelationalParser.FunctionArgContext ctx);

    @Override
    Expression visitNamedFunctionArg(RelationalParser.NamedFunctionArgContext ctx);

    @Override
    Expression visitNotExpression(RelationalParser.NotExpressionContext ctx);

    @Override
    Expression visitLogicalExpression(RelationalParser.LogicalExpressionContext ctx);

    @Override
    Expression visitPredicatedExpression(RelationalParser.PredicatedExpressionContext ctx);

    @Override
    Expression visitBinaryComparisonPredicate(RelationalParser.BinaryComparisonPredicateContext ctx);

    @Override
    Expression visitSubscriptExpression(RelationalParser.SubscriptExpressionContext ctx);

    @Override
    Expression visitInList(RelationalParser.InListContext ctx);

    @Override
    Object visitConstantExpressionAtom(RelationalParser.ConstantExpressionAtomContext ctx);

    @Override
    Expression visitFunctionCallExpressionAtom(RelationalParser.FunctionCallExpressionAtomContext ctx);

    @Override
    Object visitFullColumnNameExpressionAtom(RelationalParser.FullColumnNameExpressionAtomContext ctx);

    @Override
    Expression visitBitExpressionAtom(RelationalParser.BitExpressionAtomContext ctx);

    @Override
    Expression visitPreparedStatementParameterAtom(RelationalParser.PreparedStatementParameterAtomContext ctx);

    @Override
    Object visitRecordConstructorExpressionAtom(RelationalParser.RecordConstructorExpressionAtomContext ctx);

    @Override
    Object visitArrayConstructorExpressionAtom(RelationalParser.ArrayConstructorExpressionAtomContext ctx);

    @Override
    Expression visitMathExpressionAtom(RelationalParser.MathExpressionAtomContext ctx);

    @Override
    Expression visitExistsExpressionAtom(RelationalParser.ExistsExpressionAtomContext ctx);

    @Override
    Expression visitPreparedStatementParameter(RelationalParser.PreparedStatementParameterContext ctx);

    @Override
    Object visitUnaryOperator(RelationalParser.UnaryOperatorContext ctx);

    @Override
    Object visitComparisonOperator(RelationalParser.ComparisonOperatorContext ctx);

    @Override
    Object visitLogicalOperator(RelationalParser.LogicalOperatorContext ctx);

    @Override
    Object visitBitOperator(RelationalParser.BitOperatorContext ctx);

    @Override
    Object visitMathOperator(RelationalParser.MathOperatorContext ctx);

    @Override
    Object visitJsonOperator(RelationalParser.JsonOperatorContext ctx);

    @Override
    Object visitCharsetNameBase(RelationalParser.CharsetNameBaseContext ctx);

    @Override
    Object visitIntervalTypeBase(RelationalParser.IntervalTypeBaseContext ctx);

    @Override
    Object visitKeywordsCanBeId(RelationalParser.KeywordsCanBeIdContext ctx);

    @Override
    Object visitFunctionNameBase(RelationalParser.FunctionNameBaseContext ctx);

    @Override
    Object visitFunctionNameKeyword(RelationalParser.FunctionNameKeywordContext ctx);

    @Override
    Object visitExecuteContinuationStatement(RelationalParser.ExecuteContinuationStatementContext ctx);
}
