/*
 * TypedVisitor.java
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
import com.apple.foundationdb.relational.generated.RelationalParserVisitor;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.StringTrieNode;

import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    @Nonnull
    Object visitRoot(@Nonnull RelationalParser.RootContext ctx);

    @Override
    @Nonnull
    Object visitSqlStatements(@Nonnull RelationalParser.SqlStatementsContext ctx);

    @Override
    @Nonnull
    Object visitSqlStatement(@Nonnull RelationalParser.SqlStatementContext ctx);

    @Override
    @Nonnull
    Object visitDdlStatement(@Nonnull RelationalParser.DdlStatementContext ctx);

    @Override
    @Nonnull
    QueryPlan.LogicalQueryPlan visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx);

    @Override
    @Nonnull
    Object visitTransactionStatement(@Nonnull RelationalParser.TransactionStatementContext ctx);

    @Override
    @Nonnull
    Object visitPreparedStatement(@Nonnull RelationalParser.PreparedStatementContext ctx);

    @Override
    @Nonnull
    Object visitAdministrationStatement(@Nonnull RelationalParser.AdministrationStatementContext ctx);

    @Override
    @Nonnull
    Object visitUtilityStatement(@Nonnull RelationalParser.UtilityStatementContext ctx);

    @Override
    @Nonnull
    Object visitTemplateClause(@Nonnull RelationalParser.TemplateClauseContext ctx);

    @Override
    @Nonnull
    ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx);

    @Override
    @Nonnull
    ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx);

    @Override
    @Nonnull
    ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx);

    @Override
    @Nonnull
    Object visitOptionsClause(@Nonnull RelationalParser.OptionsClauseContext ctx);

    @Override
    @Nonnull
    Object visitOption(@Nonnull RelationalParser.OptionContext ctx);

    @Override
    @Nonnull
    ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx);

    @Override
    @Nonnull
    ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx);

    @Override
    @Nonnull
    ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx);

    @Override
    @Nonnull
    RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx);

    @Override
    @Nonnull
    RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx);

    @Override
    @Nonnull
    Object visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx);

    @Override
    @Nonnull
    DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx);

    @Override
    @Nonnull
    DataType visitPrimitiveType(@Nonnull RelationalParser.PrimitiveTypeContext ctx);

    @Override
    @Nonnull
    Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx);

    @Override
    @Nonnull
    Object visitPrimaryKeyDefinition(@Nonnull RelationalParser.PrimaryKeyDefinitionContext ctx);

    @Override
    @Nonnull
    DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx);

    @Override
    @Nonnull
    RecordLayerIndex visitIndexDefinition(@Nonnull RelationalParser.IndexDefinitionContext ctx);

    @Override
    @Nonnull
    Object visitCharSet(@Nonnull RelationalParser.CharSetContext ctx);

    @Override
    @Nonnull
    Object visitIntervalType(@Nonnull RelationalParser.IntervalTypeContext ctx);

    @Override
    @Nonnull
    Object visitSchemaId(@Nonnull RelationalParser.SchemaIdContext ctx);

    @Override
    @Nonnull
    Object visitPath(@Nonnull RelationalParser.PathContext ctx);

    @Override
    @Nonnull
    Object visitSchemaTemplateId(@Nonnull RelationalParser.SchemaTemplateIdContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitDeleteStatement(@Nonnull RelationalParser.DeleteStatementContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitInsertStatement(@Nonnull RelationalParser.InsertStatementContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitSelectStatementWithContinuation(@Nonnull RelationalParser.SelectStatementWithContinuationContext ctx);

    @Override
    @Nonnull
    Expression visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitSimpleSelect(@Nonnull RelationalParser.SimpleSelectContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitParenthesisSelect(@Nonnull RelationalParser.ParenthesisSelectContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitUnionSimpleSelect(RelationalParser.UnionSimpleSelectContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitParenthesisUnionSimpleSelect(RelationalParser.ParenthesisUnionSimpleSelectContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitParenthesisUnionParenthesisSelect(RelationalParser.ParenthesisUnionParenthesisSelectContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitUnionParenthesisSelect(@Nonnull RelationalParser.UnionParenthesisSelectContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitInsertStatementValueSelect(@Nonnull RelationalParser.InsertStatementValueSelectContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitInsertStatementValueValues(@Nonnull RelationalParser.InsertStatementValueValuesContext ctx);

    @Override
    @Nonnull
    Expressions visitUpdatedElement(@Nonnull RelationalParser.UpdatedElementContext ctx);

    @Override
    @Nonnull
    Object visitAssignmentField(@Nonnull RelationalParser.AssignmentFieldContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitUpdateStatement(@Nonnull RelationalParser.UpdateStatementContext ctx);

    @Override
    @Nonnull
    Pair<Boolean, Expressions> visitOrderByClause(@Nonnull RelationalParser.OrderByClauseContext ctx);

    @Override
    @Nonnull
    Pair<Expression, Boolean> visitOrderByExpression(@Nonnull RelationalParser.OrderByExpressionContext ctx);

    @Override
    @Nullable
    Void visitTableSources(@Nonnull RelationalParser.TableSourcesContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitTableSourceBase(@Nonnull RelationalParser.TableSourceBaseContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitAtomTableItem(@Nonnull RelationalParser.AtomTableItemContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitSubqueryTableItem(@Nonnull RelationalParser.SubqueryTableItemContext ctx);

    @Override
    @Nonnull
    Set<String> visitIndexHint(@Nonnull RelationalParser.IndexHintContext ctx);

    @Override
    @Nonnull
    Object visitIndexHintType(@Nonnull RelationalParser.IndexHintTypeContext ctx);

    @Override
    @Nonnull
    Object visitInnerJoin(@Nonnull RelationalParser.InnerJoinContext ctx);

    @Override
    @Nonnull
    Object visitStraightJoin(@Nonnull RelationalParser.StraightJoinContext ctx);

    @Override
    @Nonnull
    Object visitOuterJoin(@Nonnull RelationalParser.OuterJoinContext ctx);

    @Override
    @Nonnull
    Object visitNaturalJoin(@Nonnull RelationalParser.NaturalJoinContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitQueryExpression(@Nonnull RelationalParser.QueryExpressionContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitQuerySpecification(@Nonnull RelationalParser.QuerySpecificationContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitUnionStatement(@Nonnull RelationalParser.UnionStatementContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitUnionSelectSpecification(RelationalParser.UnionSelectSpecificationContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitUnionSelectExpression(RelationalParser.UnionSelectExpressionContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitParenthesisUnionSelectSpecification(RelationalParser.ParenthesisUnionSelectSpecificationContext ctx);

    @Override
    @Nonnull
    LogicalOperator visitParenthesisUnionSelectExpression(RelationalParser.ParenthesisUnionSelectExpressionContext ctx);

    @Override
    @Nonnull
    Expressions visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx);

    @Override
    @Nonnull
    Expression visitSelectStarElement(@Nonnull RelationalParser.SelectStarElementContext ctx);

    @Override
    @Nonnull
    Object visitSelectQualifierStarElement(@Nonnull RelationalParser.SelectQualifierStarElementContext ctx);

    @Override
    @Nonnull
    Expression visitSelectExpressionElement(@Nonnull RelationalParser.SelectExpressionElementContext ctx);

    @Override
    @Nullable
    Void visitFromClause(@Nonnull RelationalParser.FromClauseContext ctx);

    @Override
    @Nonnull
    Expressions visitGroupByClause(@Nonnull RelationalParser.GroupByClauseContext ctx);

    @Override
    @Nonnull
    Expression visitWhereExpr(@Nonnull RelationalParser.WhereExprContext ctx);

    @Override
    @Nonnull
    Expression visitHavingClause(@Nonnull RelationalParser.HavingClauseContext ctx);

    @Override
    @Nonnull
    Expression visitGroupByItem(@Nonnull RelationalParser.GroupByItemContext ctx);

    @Override
    @Nonnull
    Expression visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx);

    @Override
    @Nonnull
    Expression visitLimitClauseAtom(@Nonnull RelationalParser.LimitClauseAtomContext ctx);

    @Override
    @Nonnull
    Object visitQueryOptions(@Nonnull RelationalParser.QueryOptionsContext ctx);

    @Override
    @Nonnull
    Object visitQueryOption(@Nonnull RelationalParser.QueryOptionContext ctx);

    @Override
    @Nonnull
    Object visitStartTransaction(@Nonnull RelationalParser.StartTransactionContext ctx);

    @Override
    @Nonnull
    Object visitCommitStatement(@Nonnull RelationalParser.CommitStatementContext ctx);

    @Override
    @Nonnull
    Object visitRollbackStatement(@Nonnull RelationalParser.RollbackStatementContext ctx);

    @Override
    @Nonnull
    Object visitSetAutocommitStatement(@Nonnull RelationalParser.SetAutocommitStatementContext ctx);

    @Override
    @Nonnull
    Object visitSetTransactionStatement(@Nonnull RelationalParser.SetTransactionStatementContext ctx);

    @Override
    @Nonnull
    Object visitTransactionOption(@Nonnull RelationalParser.TransactionOptionContext ctx);

    @Override
    @Nonnull
    Object visitTransactionLevel(@Nonnull RelationalParser.TransactionLevelContext ctx);

    @Override
    @Nonnull
    Object visitPrepareStatement(@Nonnull RelationalParser.PrepareStatementContext ctx);

    @Override
    @Nonnull
    Object visitExecuteStatement(@Nonnull RelationalParser.ExecuteStatementContext ctx);

    @Override
    @Nonnull
    QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(@Nonnull RelationalParser.ShowDatabasesStatementContext ctx);

    @Override
    @Nonnull
    QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(@Nonnull RelationalParser.ShowSchemaTemplatesStatementContext ctx);

    @Override
    @Nonnull
    Object visitSetVariable(@Nonnull RelationalParser.SetVariableContext ctx);

    @Override
    @Nonnull
    Object visitSetCharset(@Nonnull RelationalParser.SetCharsetContext ctx);

    @Override
    @Nonnull
    Object visitSetNames(@Nonnull RelationalParser.SetNamesContext ctx);

    @Override
    @Nonnull
    Object visitSetTransaction(@Nonnull RelationalParser.SetTransactionContext ctx);

    @Override
    @Nonnull
    Object visitSetAutocommit(@Nonnull RelationalParser.SetAutocommitContext ctx);

    @Override
    @Nonnull
    Object visitSetNewValueInsideTrigger(@Nonnull RelationalParser.SetNewValueInsideTriggerContext ctx);

    @Override
    @Nonnull
    Object visitVariableClause(@Nonnull RelationalParser.VariableClauseContext ctx);

    @Override
    @Nonnull
    Object visitKillStatement(@Nonnull RelationalParser.KillStatementContext ctx);

    @Override
    @Nonnull
    Object visitResetStatement(@Nonnull RelationalParser.ResetStatementContext ctx);

    @Override
    @Nonnull
    Object visitTableIndexes(@Nonnull RelationalParser.TableIndexesContext ctx);

    @Override
    @Nonnull
    Object visitLoadedTableIndexes(@Nonnull RelationalParser.LoadedTableIndexesContext ctx);

    @Override
    @Nonnull
    QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(@Nonnull RelationalParser.SimpleDescribeSchemaStatementContext ctx);

    @Override
    @Nonnull
    QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(@Nonnull RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx);

    @Override
    @Nonnull
    QueryPlan.LogicalQueryPlan visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx);

    @Override
    @Nonnull
    Object visitHelpStatement(@Nonnull RelationalParser.HelpStatementContext ctx);

    @Override
    @Nonnull
    Object visitDescribeStatements(@Nonnull RelationalParser.DescribeStatementsContext ctx);

    @Override
    @Nonnull
    Object visitDescribeConnection(@Nonnull RelationalParser.DescribeConnectionContext ctx);

    @Override
    @Nonnull
    Identifier visitFullId(@Nonnull RelationalParser.FullIdContext ctx);

    @Override
    @Nonnull
    Identifier visitTableName(@Nonnull RelationalParser.TableNameContext ctx);

    @Override
    @Nonnull
    Expression visitFullColumnName(@Nonnull RelationalParser.FullColumnNameContext ctx);

    @Override
    @Nonnull
    Identifier visitIndexColumnName(@Nonnull RelationalParser.IndexColumnNameContext ctx);

    @Override
    @Nonnull
    Identifier visitCharsetName(@Nonnull RelationalParser.CharsetNameContext ctx);

    @Override
    @Nonnull
    Identifier visitCollationName(@Nonnull RelationalParser.CollationNameContext ctx);

    @Override
    @Nonnull
    Identifier visitUid(@Nonnull RelationalParser.UidContext ctx);

    @Override
    @Nonnull
    Identifier visitSimpleId(@Nonnull RelationalParser.SimpleIdContext ctx);

    @Override
    @Nonnull
    Object visitNullNotnull(@Nonnull RelationalParser.NullNotnullContext ctx);

    @Override
    @Nonnull
    Expression visitDecimalLiteral(@Nonnull RelationalParser.DecimalLiteralContext ctx);

    @Override
    @Nonnull
    Expression visitStringLiteral(@Nonnull RelationalParser.StringLiteralContext ctx);

    @Override
    @Nonnull
    Expression visitBooleanLiteral(@Nonnull RelationalParser.BooleanLiteralContext ctx);

    @Override
    @Nonnull
    Expression visitBytesLiteral(@Nonnull RelationalParser.BytesLiteralContext ctx);

    @Override
    @Nonnull
    Expression visitNullLiteral(@Nonnull RelationalParser.NullLiteralContext ctx);

    @Override
    @Nonnull
    Expression visitStringConstant(@Nonnull RelationalParser.StringConstantContext ctx);

    @Override
    @Nonnull
    Expression visitDecimalConstant(@Nonnull RelationalParser.DecimalConstantContext ctx);

    @Override
    @Nonnull
    Expression visitNegativeDecimalConstant(@Nonnull RelationalParser.NegativeDecimalConstantContext ctx);

    @Override
    @Nonnull
    Expression visitBytesConstant(@Nonnull RelationalParser.BytesConstantContext ctx);

    @Override
    @Nonnull
    Expression visitBooleanConstant(@Nonnull RelationalParser.BooleanConstantContext ctx);

    @Override
    @Nonnull
    Expression visitBitStringConstant(@Nonnull RelationalParser.BitStringConstantContext ctx);

    @Override
    @Nonnull
    Expression visitNullConstant(@Nonnull RelationalParser.NullConstantContext ctx);

    @Override
    @Nonnull
    Object visitStringDataType(@Nonnull RelationalParser.StringDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitNationalStringDataType(@Nonnull RelationalParser.NationalStringDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitNationalVaryingStringDataType(@Nonnull RelationalParser.NationalVaryingStringDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitDimensionDataType(@Nonnull RelationalParser.DimensionDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitSimpleDataType(@Nonnull RelationalParser.SimpleDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitCollectionDataType(@Nonnull RelationalParser.CollectionDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitSpatialDataType(@Nonnull RelationalParser.SpatialDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitLongVarcharDataType(@Nonnull RelationalParser.LongVarcharDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitLongVarbinaryDataType(@Nonnull RelationalParser.LongVarbinaryDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitCollectionOptions(@Nonnull RelationalParser.CollectionOptionsContext ctx);

    @Override
    @Nonnull
    Object visitConvertedDataType(@Nonnull RelationalParser.ConvertedDataTypeContext ctx);

    @Override
    @Nonnull
    Object visitLengthOneDimension(@Nonnull RelationalParser.LengthOneDimensionContext ctx);

    @Override
    @Nonnull
    Object visitLengthTwoDimension(@Nonnull RelationalParser.LengthTwoDimensionContext ctx);

    @Override
    @Nonnull
    Object visitLengthTwoOptionalDimension(@Nonnull RelationalParser.LengthTwoOptionalDimensionContext ctx);

    @Override
    @Nonnull
    List<Identifier> visitUidList(@Nonnull RelationalParser.UidListContext ctx);

    @Override
    @Nonnull
    Pair<String, StringTrieNode> visitUidWithNestings(@Nonnull RelationalParser.UidWithNestingsContext ctx);

    @Override
    @Nonnull
    StringTrieNode visitUidListWithNestingsInParens(@Nonnull RelationalParser.UidListWithNestingsInParensContext ctx);

    @Override
    @Nonnull
    StringTrieNode visitUidListWithNestings(@Nonnull RelationalParser.UidListWithNestingsContext ctx);

    @Override
    @Nonnull
    Object visitTables(@Nonnull RelationalParser.TablesContext ctx);

    @Override
    @Nonnull
    Object visitIndexColumnNames(@Nonnull RelationalParser.IndexColumnNamesContext ctx);

    @Override
    @Nonnull
    Expressions visitExpressions(@Nonnull RelationalParser.ExpressionsContext ctx);

    @Override
    @Nonnull
    Object visitExpressionsWithDefaults(@Nonnull RelationalParser.ExpressionsWithDefaultsContext ctx);

    @Override
    @Nonnull
    Expression visitRecordConstructorForInsert(@Nonnull RelationalParser.RecordConstructorForInsertContext ctx);

    @Override
    @Nonnull
    Expression visitRecordConstructor(@Nonnull RelationalParser.RecordConstructorContext ctx);

    @Override
    @Nonnull
    Object visitOfTypeClause(@Nonnull RelationalParser.OfTypeClauseContext ctx);

    @Override
    @Nonnull
    Expression visitArrayConstructor(@Nonnull RelationalParser.ArrayConstructorContext ctx);

    @Override
    @Nonnull
    Object visitUserVariables(@Nonnull RelationalParser.UserVariablesContext ctx);

    @Override
    @Nonnull
    Object visitDefaultValue(@Nonnull RelationalParser.DefaultValueContext ctx);

    @Override
    @Nonnull
    Object visitCurrentTimestamp(@Nonnull RelationalParser.CurrentTimestampContext ctx);

    @Override
    @Nonnull
    Object visitExpressionOrDefault(@Nonnull RelationalParser.ExpressionOrDefaultContext ctx);

    @Override
    @Nonnull
    Expression visitExpressionWithName(@Nonnull RelationalParser.ExpressionWithNameContext ctx);

    @Override
    @Nonnull
    Expression visitExpressionWithOptionalName(@Nonnull RelationalParser.ExpressionWithOptionalNameContext ctx);

    @Override
    @Nonnull
    Object visitIfExists(@Nonnull RelationalParser.IfExistsContext ctx);

    @Override
    @Nonnull
    Object visitIfNotExists(@Nonnull RelationalParser.IfNotExistsContext ctx);

    @Override
    @Nonnull
    Expression visitAggregateFunctionCall(@Nonnull RelationalParser.AggregateFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitSpecificFunctionCall(@Nonnull RelationalParser.SpecificFunctionCallContext ctx);

    @Override
    @Nonnull
    Expression visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitSimpleFunctionCall(@Nonnull RelationalParser.SimpleFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitDataTypeFunctionCall(@Nonnull RelationalParser.DataTypeFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitValuesFunctionCall(@Nonnull RelationalParser.ValuesFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitCaseExpressionFunctionCall(@Nonnull RelationalParser.CaseExpressionFunctionCallContext ctx);

    @Override
    @Nonnull
    Expression visitCaseFunctionCall(@Nonnull RelationalParser.CaseFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitCharFunctionCall(@Nonnull RelationalParser.CharFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitPositionFunctionCall(@Nonnull RelationalParser.PositionFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitSubstrFunctionCall(@Nonnull RelationalParser.SubstrFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitTrimFunctionCall(@Nonnull RelationalParser.TrimFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitWeightFunctionCall(@Nonnull RelationalParser.WeightFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitExtractFunctionCall(@Nonnull RelationalParser.ExtractFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitGetFormatFunctionCall(@Nonnull RelationalParser.GetFormatFunctionCallContext ctx);

    @Override
    @Nonnull
    Object visitCaseFuncAlternative(@Nonnull RelationalParser.CaseFuncAlternativeContext ctx);

    @Override
    @Nonnull
    Object visitLevelWeightList(@Nonnull RelationalParser.LevelWeightListContext ctx);

    @Override
    @Nonnull
    Object visitLevelWeightRange(@Nonnull RelationalParser.LevelWeightRangeContext ctx);

    @Override
    @Nonnull
    Object visitLevelInWeightListElement(@Nonnull RelationalParser.LevelInWeightListElementContext ctx);

    @Override
    @Nonnull
    Expression visitAggregateWindowedFunction(@Nonnull RelationalParser.AggregateWindowedFunctionContext ctx);

    @Override
    @Nonnull
    Object visitNonAggregateWindowedFunction(@Nonnull RelationalParser.NonAggregateWindowedFunctionContext ctx);

    @Override
    @Nonnull
    Object visitOverClause(@Nonnull RelationalParser.OverClauseContext ctx);

    @Override
    @Nonnull
    Object visitWindowName(@Nonnull RelationalParser.WindowNameContext ctx);

    @Override
    @Nonnull
    Object visitScalarFunctionName(@Nonnull RelationalParser.ScalarFunctionNameContext ctx);

    @Override
    @Nonnull
    Expressions visitFunctionArgs(@Nonnull RelationalParser.FunctionArgsContext ctx);

    @Override
    @Nonnull
    Object visitFunctionArg(@Nonnull RelationalParser.FunctionArgContext ctx);

    @Override
    @Nonnull
    Expression visitIsExpression(@Nonnull RelationalParser.IsExpressionContext ctx);

    @Override
    @Nonnull
    Expression visitNotExpression(@Nonnull RelationalParser.NotExpressionContext ctx);

    @Override
    @Nonnull
    Expression visitLikePredicate(@Nonnull RelationalParser.LikePredicateContext ctx);

    @Override
    @Nonnull
    Expression visitLogicalExpression(@Nonnull RelationalParser.LogicalExpressionContext ctx);

    @Override
    @Nonnull
    Object visitPredicateExpression(@Nonnull RelationalParser.PredicateExpressionContext ctx);

    @Override
    @Nonnull
    Object visitExpressionAtomPredicate(@Nonnull RelationalParser.ExpressionAtomPredicateContext ctx);

    @Override
    @Nonnull
    Expression visitBinaryComparisonPredicate(@Nonnull RelationalParser.BinaryComparisonPredicateContext ctx);

    @Override
    @Nonnull
    Expression visitInPredicate(@Nonnull RelationalParser.InPredicateContext ctx);

    @Override
    @Nonnull
    Expression visitInList(@Nonnull RelationalParser.InListContext ctx);

    @Override
    @Nonnull
    Object visitJsonExpressionAtom(@Nonnull RelationalParser.JsonExpressionAtomContext ctx);

    @Override
    @Nonnull
    Expression visitSubqueryExpressionAtom(@Nonnull RelationalParser.SubqueryExpressionAtomContext ctx);

    @Override
    @Nonnull
    Object visitConstantExpressionAtom(@Nonnull RelationalParser.ConstantExpressionAtomContext ctx);

    @Override
    @Nonnull
    Expression visitFunctionCallExpressionAtom(@Nonnull RelationalParser.FunctionCallExpressionAtomContext ctx);

    @Override
    @Nonnull
    Object visitFullColumnNameExpressionAtom(@Nonnull RelationalParser.FullColumnNameExpressionAtomContext ctx);

    @Override
    @Nonnull
    Expression visitBitExpressionAtom(@Nonnull RelationalParser.BitExpressionAtomContext ctx);

    @Override
    @Nonnull
    Expression visitPreparedStatementParameterAtom(@Nonnull RelationalParser.PreparedStatementParameterAtomContext ctx);

    @Override
    @Nonnull
    Object visitRecordConstructorExpressionAtom(@Nonnull RelationalParser.RecordConstructorExpressionAtomContext ctx);

    @Override
    @Nonnull
    Object visitArrayConstructorExpressionAtom(@Nonnull RelationalParser.ArrayConstructorExpressionAtomContext ctx);

    @Override
    @Nonnull
    Expression visitMathExpressionAtom(@Nonnull RelationalParser.MathExpressionAtomContext ctx);

    @Override
    @Nonnull
    Expression visitExistsExpressionAtom(@Nonnull RelationalParser.ExistsExpressionAtomContext ctx);

    @Override
    @Nonnull
    Object visitIntervalExpressionAtom(@Nonnull RelationalParser.IntervalExpressionAtomContext ctx);

    @Override
    @Nonnull
    Expression visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx);

    @Override
    @Nonnull
    Object visitUnaryOperator(@Nonnull RelationalParser.UnaryOperatorContext ctx);

    @Override
    @Nonnull
    Object visitComparisonOperator(@Nonnull RelationalParser.ComparisonOperatorContext ctx);

    @Override
    @Nonnull
    Object visitLogicalOperator(@Nonnull RelationalParser.LogicalOperatorContext ctx);

    @Override
    @Nonnull
    Object visitBitOperator(@Nonnull RelationalParser.BitOperatorContext ctx);

    @Override
    @Nonnull
    Object visitMathOperator(@Nonnull RelationalParser.MathOperatorContext ctx);

    @Override
    @Nonnull
    Object visitJsonOperator(@Nonnull RelationalParser.JsonOperatorContext ctx);

    @Override
    @Nonnull
    Object visitCharsetNameBase(@Nonnull RelationalParser.CharsetNameBaseContext ctx);

    @Override
    @Nonnull
    Object visitIntervalTypeBase(@Nonnull RelationalParser.IntervalTypeBaseContext ctx);

    @Override
    @Nonnull
    Object visitKeywordsCanBeId(@Nonnull RelationalParser.KeywordsCanBeIdContext ctx);

    @Override
    @Nonnull
    Object visitFunctionNameBase(@Nonnull RelationalParser.FunctionNameBaseContext ctx);

    @Override
    @Nonnull
    Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx);
}
