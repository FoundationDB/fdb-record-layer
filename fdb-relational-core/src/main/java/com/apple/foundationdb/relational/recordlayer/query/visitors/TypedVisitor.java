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

import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.generated.RelationalParserVisitor;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperators;
import com.apple.foundationdb.relational.recordlayer.query.OrderByExpression;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.StringTrieNode;

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

    @Nonnull
    @Override
    Object visitRoot(@Nonnull RelationalParser.RootContext ctx);

    @Nonnull
    @Override
    Object visitStatements(@Nonnull RelationalParser.StatementsContext ctx);

    @Nullable
    @Override
    Object visitStatement(@Nonnull RelationalParser.StatementContext ctx);

    @Nonnull
    @Override
    QueryPlan.LogicalQueryPlan visitDmlStatement(@Nonnull RelationalParser.DmlStatementContext ctx);

    @Nonnull
    @Override
    Object visitDdlStatement(RelationalParser.DdlStatementContext ctx);

    @Nonnull
    @Override
    Object visitTransactionStatement(@Nonnull RelationalParser.TransactionStatementContext ctx);

    @Nonnull
    @Override
    Object visitPreparedStatement(@Nonnull RelationalParser.PreparedStatementContext ctx);

    @Nonnull
    @Override
    Object visitAdministrationStatement(@Nonnull RelationalParser.AdministrationStatementContext ctx);

    @Nonnull
    @Override
    Object visitUtilityStatement(@Nonnull RelationalParser.UtilityStatementContext ctx);

    @Nonnull
    @Override
    Object visitTemplateClause(@Nonnull RelationalParser.TemplateClauseContext ctx);

    @Nonnull
    @Override
    ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx);

    @Nonnull
    @Override
    ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx);

    @Nonnull
    @Override
    ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx);

    @Nonnull
    @Override
    Object visitOptionsClause(@Nonnull RelationalParser.OptionsClauseContext ctx);

    @Nonnull
    @Override
    Object visitOption(@Nonnull RelationalParser.OptionContext ctx);

    @Nonnull
    @Override
    ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx);

    @Nonnull
    @Override
    ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx);

    @Nonnull
    @Override
    ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx);

    @Nonnull
    @Override
    RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx);

    @Nonnull
    @Override
    RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx);

    @Nonnull
    @Override
    Object visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx);

    @Nonnull
    @Override
    DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx);

    @Nonnull
    @Override
    DataType visitPrimitiveType(@Nonnull RelationalParser.PrimitiveTypeContext ctx);

    @Nonnull
    @Override
    Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx);

    @Nonnull
    @Override
    Object visitPrimaryKeyDefinition(@Nonnull RelationalParser.PrimaryKeyDefinitionContext ctx);

    @Override
    @Nonnull
    List<Identifier> visitFullIdList(RelationalParser.FullIdListContext ctx);

    @Nonnull
    @Override
    DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx);

    @Nonnull
    @Override
    RecordLayerIndex visitIndexDefinition(@Nonnull RelationalParser.IndexDefinitionContext ctx);

    @Nonnull
    @Override
    Object visitFunctionDefinition(@Nonnull RelationalParser.FunctionDefinitionContext ctx);

    @Override
    Object visitIndexAttributes(RelationalParser.IndexAttributesContext ctx);

    @Override
    Object visitIndexAttribute(RelationalParser.IndexAttributeContext ctx);

    @Nonnull
    @Override
    Object visitCharSet(@Nonnull RelationalParser.CharSetContext ctx);

    @Nonnull
    @Override
    Object visitIntervalType(@Nonnull RelationalParser.IntervalTypeContext ctx);

    @Nonnull
    @Override
    Object visitSchemaId(@Nonnull RelationalParser.SchemaIdContext ctx);

    @Nonnull
    @Override
    Object visitPath(@Nonnull RelationalParser.PathContext ctx);

    @Nonnull
    @Override
    Object visitSchemaTemplateId(@Nonnull RelationalParser.SchemaTemplateIdContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitDeleteStatement(@Nonnull RelationalParser.DeleteStatementContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitInsertStatement(@Nonnull RelationalParser.InsertStatementContext ctx);

    @Nonnull
    @Override
    QueryPlan.LogicalQueryPlan visitSelectStatement(RelationalParser.SelectStatementContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitQuery(@Nonnull RelationalParser.QueryContext ctx);

    @Nonnull
    @Override
    LogicalOperators visitCtes(RelationalParser.CtesContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitNamedQuery(RelationalParser.NamedQueryContext ctx);

    @Nonnull
    @Override
    Expression visitContinuation(RelationalParser.ContinuationContext ctx);

    @Nonnull
    @Override
    Expression visitContinuationAtom(@Nonnull RelationalParser.ContinuationAtomContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitQueryTermDefault(@Nonnull RelationalParser.QueryTermDefaultContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitSetQuery(RelationalParser.SetQueryContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitInsertStatementValueSelect(@Nonnull RelationalParser.InsertStatementValueSelectContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitInsertStatementValueValues(@Nonnull RelationalParser.InsertStatementValueValuesContext ctx);

    @Nonnull
    @Override
    Expressions visitUpdatedElement(@Nonnull RelationalParser.UpdatedElementContext ctx);

    @Nonnull
    @Override
    Object visitAssignmentField(@Nonnull RelationalParser.AssignmentFieldContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitUpdateStatement(@Nonnull RelationalParser.UpdateStatementContext ctx);

    @Nonnull
    @Override
    List<OrderByExpression> visitOrderByClause(@Nonnull RelationalParser.OrderByClauseContext ctx);

    @Nonnull
    @Override
    OrderByExpression visitOrderByExpression(@Nonnull RelationalParser.OrderByExpressionContext ctx);

    @Override
    @Nullable
    Void visitTableSources(@Nonnull RelationalParser.TableSourcesContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitTableSourceBase(@Nonnull RelationalParser.TableSourceBaseContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitAtomTableItem(@Nonnull RelationalParser.AtomTableItemContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitSubqueryTableItem(@Nonnull RelationalParser.SubqueryTableItemContext ctx);

    @Nonnull
    @Override
    Set<String> visitIndexHint(@Nonnull RelationalParser.IndexHintContext ctx);

    @Nonnull
    @Override
    Object visitIndexHintType(@Nonnull RelationalParser.IndexHintTypeContext ctx);

    @Nonnull
    @Override
    Object visitInnerJoin(@Nonnull RelationalParser.InnerJoinContext ctx);

    @Nonnull
    @Override
    Object visitStraightJoin(@Nonnull RelationalParser.StraightJoinContext ctx);

    @Nonnull
    @Override
    Object visitOuterJoin(@Nonnull RelationalParser.OuterJoinContext ctx);

    @Nonnull
    @Override
    Object visitNaturalJoin(@Nonnull RelationalParser.NaturalJoinContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitSimpleTable(@Nonnull RelationalParser.SimpleTableContext ctx);

    @Nonnull
    @Override
    LogicalOperator visitParenthesisQuery(RelationalParser.ParenthesisQueryContext ctx);

    @Nonnull
    @Override
    Expressions visitSelectElements(@Nonnull RelationalParser.SelectElementsContext ctx);

    @Nonnull
    @Override
    Expression visitSelectStarElement(@Nonnull RelationalParser.SelectStarElementContext ctx);

    @Nonnull
    @Override
    Object visitSelectQualifierStarElement(@Nonnull RelationalParser.SelectQualifierStarElementContext ctx);

    @Nonnull
    @Override
    Expression visitSelectExpressionElement(@Nonnull RelationalParser.SelectExpressionElementContext ctx);

    @Override
    @Nullable
    Void visitFromClause(@Nonnull RelationalParser.FromClauseContext ctx);

    @Nonnull
    @Override
    Expressions visitGroupByClause(@Nonnull RelationalParser.GroupByClauseContext ctx);

    @Nonnull
    @Override
    Expression visitWhereExpr(@Nonnull RelationalParser.WhereExprContext ctx);

    @Nonnull
    @Override
    Expression visitHavingClause(@Nonnull RelationalParser.HavingClauseContext ctx);

    @Nonnull
    @Override
    Expression visitGroupByItem(@Nonnull RelationalParser.GroupByItemContext ctx);

    @Nonnull
    @Override
    Expression visitLimitClause(@Nonnull RelationalParser.LimitClauseContext ctx);

    @Nonnull
    @Override
    Expression visitLimitClauseAtom(@Nonnull RelationalParser.LimitClauseAtomContext ctx);

    @Nonnull
    @Override
    Object visitQueryOptions(@Nonnull RelationalParser.QueryOptionsContext ctx);

    @Nonnull
    @Override
    Object visitQueryOption(@Nonnull RelationalParser.QueryOptionContext ctx);

    @Nonnull
    @Override
    Object visitStartTransaction(@Nonnull RelationalParser.StartTransactionContext ctx);

    @Nonnull
    @Override
    Object visitCommitStatement(@Nonnull RelationalParser.CommitStatementContext ctx);

    @Nonnull
    @Override
    Object visitRollbackStatement(@Nonnull RelationalParser.RollbackStatementContext ctx);

    @Nonnull
    @Override
    Object visitSetAutocommitStatement(@Nonnull RelationalParser.SetAutocommitStatementContext ctx);

    @Nonnull
    @Override
    Object visitSetTransactionStatement(@Nonnull RelationalParser.SetTransactionStatementContext ctx);

    @Nonnull
    @Override
    Object visitTransactionOption(@Nonnull RelationalParser.TransactionOptionContext ctx);

    @Nonnull
    @Override
    Object visitTransactionLevel(@Nonnull RelationalParser.TransactionLevelContext ctx);

    @Nonnull
    @Override
    Object visitPrepareStatement(@Nonnull RelationalParser.PrepareStatementContext ctx);

    @Nonnull
    @Override
    Object visitExecuteStatement(@Nonnull RelationalParser.ExecuteStatementContext ctx);

    @Nonnull
    @Override
    QueryPlan.MetadataQueryPlan visitShowDatabasesStatement(@Nonnull RelationalParser.ShowDatabasesStatementContext ctx);

    @Nonnull
    @Override
    QueryPlan.MetadataQueryPlan visitShowSchemaTemplatesStatement(@Nonnull RelationalParser.ShowSchemaTemplatesStatementContext ctx);

    @Nonnull
    @Override
    Object visitSetVariable(@Nonnull RelationalParser.SetVariableContext ctx);

    @Nonnull
    @Override
    Object visitSetCharset(@Nonnull RelationalParser.SetCharsetContext ctx);

    @Nonnull
    @Override
    Object visitSetNames(@Nonnull RelationalParser.SetNamesContext ctx);

    @Nonnull
    @Override
    Object visitSetTransaction(@Nonnull RelationalParser.SetTransactionContext ctx);

    @Nonnull
    @Override
    Object visitSetAutocommit(@Nonnull RelationalParser.SetAutocommitContext ctx);

    @Nonnull
    @Override
    Object visitSetNewValueInsideTrigger(@Nonnull RelationalParser.SetNewValueInsideTriggerContext ctx);

    @Nonnull
    @Override
    Object visitVariableClause(@Nonnull RelationalParser.VariableClauseContext ctx);

    @Nonnull
    @Override
    Object visitKillStatement(@Nonnull RelationalParser.KillStatementContext ctx);

    @Nonnull
    @Override
    Object visitResetStatement(@Nonnull RelationalParser.ResetStatementContext ctx);

    @Nonnull
    @Override
    Object visitTableIndexes(@Nonnull RelationalParser.TableIndexesContext ctx);

    @Nonnull
    @Override
    Object visitLoadedTableIndexes(@Nonnull RelationalParser.LoadedTableIndexesContext ctx);

    @Nonnull
    @Override
    QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaStatement(@Nonnull RelationalParser.SimpleDescribeSchemaStatementContext ctx);

    @Nonnull
    @Override
    QueryPlan.MetadataQueryPlan visitSimpleDescribeSchemaTemplateStatement(@Nonnull RelationalParser.SimpleDescribeSchemaTemplateStatementContext ctx);

    @Nonnull
    @Override
    QueryPlan.LogicalQueryPlan visitFullDescribeStatement(@Nonnull RelationalParser.FullDescribeStatementContext ctx);

    @Nonnull
    @Override
    Object visitHelpStatement(@Nonnull RelationalParser.HelpStatementContext ctx);

    @Nonnull
    @Override
    Object visitDescribeStatements(@Nonnull RelationalParser.DescribeStatementsContext ctx);

    @Nonnull
    @Override
    Object visitDescribeConnection(@Nonnull RelationalParser.DescribeConnectionContext ctx);

    @Nonnull
    @Override
    Identifier visitFullId(@Nonnull RelationalParser.FullIdContext ctx);

    @Nonnull
    @Override
    Identifier visitTableName(@Nonnull RelationalParser.TableNameContext ctx);

    @Nonnull
    @Override
    Expression visitFullColumnName(@Nonnull RelationalParser.FullColumnNameContext ctx);

    @Nonnull
    @Override
    Identifier visitIndexColumnName(@Nonnull RelationalParser.IndexColumnNameContext ctx);

    @Nonnull
    @Override
    Identifier visitCharsetName(@Nonnull RelationalParser.CharsetNameContext ctx);

    @Nonnull
    @Override
    Identifier visitCollationName(@Nonnull RelationalParser.CollationNameContext ctx);

    @Nonnull
    @Override
    Identifier visitUid(@Nonnull RelationalParser.UidContext ctx);

    @Nonnull
    @Override
    Identifier visitSimpleId(@Nonnull RelationalParser.SimpleIdContext ctx);

    @Nonnull
    @Override
    Object visitNullNotnull(@Nonnull RelationalParser.NullNotnullContext ctx);

    @Nonnull
    @Override
    Expression visitDecimalLiteral(@Nonnull RelationalParser.DecimalLiteralContext ctx);

    @Nonnull
    @Override
    Expression visitStringLiteral(@Nonnull RelationalParser.StringLiteralContext ctx);

    @Nonnull
    @Override
    Expression visitBooleanLiteral(@Nonnull RelationalParser.BooleanLiteralContext ctx);

    @Nonnull
    @Override
    Expression visitBytesLiteral(@Nonnull RelationalParser.BytesLiteralContext ctx);

    @Nonnull
    @Override
    Expression visitNullLiteral(@Nonnull RelationalParser.NullLiteralContext ctx);

    @Nonnull
    @Override
    Expression visitStringConstant(@Nonnull RelationalParser.StringConstantContext ctx);

    @Nonnull
    @Override
    Expression visitDecimalConstant(@Nonnull RelationalParser.DecimalConstantContext ctx);

    @Nonnull
    @Override
    Expression visitNegativeDecimalConstant(@Nonnull RelationalParser.NegativeDecimalConstantContext ctx);

    @Nonnull
    @Override
    Expression visitBytesConstant(@Nonnull RelationalParser.BytesConstantContext ctx);

    @Nonnull
    @Override
    Expression visitBooleanConstant(@Nonnull RelationalParser.BooleanConstantContext ctx);

    @Nonnull
    @Override
    Expression visitBitStringConstant(@Nonnull RelationalParser.BitStringConstantContext ctx);

    @Nonnull
    @Override
    Expression visitNullConstant(@Nonnull RelationalParser.NullConstantContext ctx);

    @Nonnull
    @Override
    Object visitStringDataType(@Nonnull RelationalParser.StringDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitNationalStringDataType(@Nonnull RelationalParser.NationalStringDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitNationalVaryingStringDataType(@Nonnull RelationalParser.NationalVaryingStringDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitDimensionDataType(@Nonnull RelationalParser.DimensionDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitSimpleDataType(@Nonnull RelationalParser.SimpleDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitCollectionDataType(@Nonnull RelationalParser.CollectionDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitSpatialDataType(@Nonnull RelationalParser.SpatialDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitLongVarcharDataType(@Nonnull RelationalParser.LongVarcharDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitLongVarbinaryDataType(@Nonnull RelationalParser.LongVarbinaryDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitCollectionOptions(@Nonnull RelationalParser.CollectionOptionsContext ctx);

    @Nonnull
    @Override
    Object visitConvertedDataType(@Nonnull RelationalParser.ConvertedDataTypeContext ctx);

    @Nonnull
    @Override
    Object visitLengthOneDimension(@Nonnull RelationalParser.LengthOneDimensionContext ctx);

    @Nonnull
    @Override
    Object visitLengthTwoDimension(@Nonnull RelationalParser.LengthTwoDimensionContext ctx);

    @Nonnull
    @Override
    Object visitLengthTwoOptionalDimension(@Nonnull RelationalParser.LengthTwoOptionalDimensionContext ctx);

    @Nonnull
    @Override
    List<Identifier> visitUidList(@Nonnull RelationalParser.UidListContext ctx);

    @Nonnull
    @Override
    NonnullPair<String, StringTrieNode> visitUidWithNestings(@Nonnull RelationalParser.UidWithNestingsContext ctx);

    @Nonnull
    @Override
    StringTrieNode visitUidListWithNestingsInParens(@Nonnull RelationalParser.UidListWithNestingsInParensContext ctx);

    @Nonnull
    @Override
    StringTrieNode visitUidListWithNestings(@Nonnull RelationalParser.UidListWithNestingsContext ctx);

    @Nonnull
    @Override
    Object visitTables(@Nonnull RelationalParser.TablesContext ctx);

    @Nonnull
    @Override
    Object visitIndexColumnNames(@Nonnull RelationalParser.IndexColumnNamesContext ctx);

    @Nonnull
    @Override
    Expressions visitExpressions(@Nonnull RelationalParser.ExpressionsContext ctx);

    @Nonnull
    @Override
    Object visitExpressionsWithDefaults(@Nonnull RelationalParser.ExpressionsWithDefaultsContext ctx);

    @Nonnull
    @Override
    Expression visitRecordConstructorForInsert(@Nonnull RelationalParser.RecordConstructorForInsertContext ctx);

    @Nonnull
    @Override
    Expression visitRecordConstructor(@Nonnull RelationalParser.RecordConstructorContext ctx);

    @Nonnull
    @Override
    Object visitOfTypeClause(@Nonnull RelationalParser.OfTypeClauseContext ctx);

    @Nonnull
    @Override
    Expression visitArrayConstructor(@Nonnull RelationalParser.ArrayConstructorContext ctx);

    @Nonnull
    @Override
    Object visitUserVariables(@Nonnull RelationalParser.UserVariablesContext ctx);

    @Nonnull
    @Override
    Object visitDefaultValue(@Nonnull RelationalParser.DefaultValueContext ctx);

    @Nonnull
    @Override
    Object visitCurrentTimestamp(@Nonnull RelationalParser.CurrentTimestampContext ctx);

    @Nonnull
    @Override
    Object visitExpressionOrDefault(@Nonnull RelationalParser.ExpressionOrDefaultContext ctx);

    @Nonnull
    @Override
    Expression visitExpressionWithName(@Nonnull RelationalParser.ExpressionWithNameContext ctx);

    @Nonnull
    @Override
    Expression visitExpressionWithOptionalName(@Nonnull RelationalParser.ExpressionWithOptionalNameContext ctx);

    @Nonnull
    @Override
    Object visitIfExists(@Nonnull RelationalParser.IfExistsContext ctx);

    @Nonnull
    @Override
    Object visitIfNotExists(@Nonnull RelationalParser.IfNotExistsContext ctx);

    @Nonnull
    @Override
    Expression visitAggregateFunctionCall(@Nonnull RelationalParser.AggregateFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitSpecificFunctionCall(@Nonnull RelationalParser.SpecificFunctionCallContext ctx);

    @Nonnull
    @Override
    Expression visitScalarFunctionCall(@Nonnull RelationalParser.ScalarFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitSimpleFunctionCall(@Nonnull RelationalParser.SimpleFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitDataTypeFunctionCall(@Nonnull RelationalParser.DataTypeFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitValuesFunctionCall(@Nonnull RelationalParser.ValuesFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitCaseExpressionFunctionCall(@Nonnull RelationalParser.CaseExpressionFunctionCallContext ctx);

    @Nonnull
    @Override
    Expression visitCaseFunctionCall(@Nonnull RelationalParser.CaseFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitCharFunctionCall(@Nonnull RelationalParser.CharFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitPositionFunctionCall(@Nonnull RelationalParser.PositionFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitSubstrFunctionCall(@Nonnull RelationalParser.SubstrFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitTrimFunctionCall(@Nonnull RelationalParser.TrimFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitWeightFunctionCall(@Nonnull RelationalParser.WeightFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitExtractFunctionCall(@Nonnull RelationalParser.ExtractFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitGetFormatFunctionCall(@Nonnull RelationalParser.GetFormatFunctionCallContext ctx);

    @Nonnull
    @Override
    Object visitCaseFuncAlternative(@Nonnull RelationalParser.CaseFuncAlternativeContext ctx);

    @Nonnull
    @Override
    Object visitLevelWeightList(@Nonnull RelationalParser.LevelWeightListContext ctx);

    @Nonnull
    @Override
    Object visitLevelWeightRange(@Nonnull RelationalParser.LevelWeightRangeContext ctx);

    @Nonnull
    @Override
    Object visitLevelInWeightListElement(@Nonnull RelationalParser.LevelInWeightListElementContext ctx);

    @Nonnull
    @Override
    Expression visitAggregateWindowedFunction(@Nonnull RelationalParser.AggregateWindowedFunctionContext ctx);

    @Nonnull
    @Override
    Object visitNonAggregateWindowedFunction(@Nonnull RelationalParser.NonAggregateWindowedFunctionContext ctx);

    @Nonnull
    @Override
    Object visitOverClause(@Nonnull RelationalParser.OverClauseContext ctx);

    @Nonnull
    @Override
    Object visitWindowName(@Nonnull RelationalParser.WindowNameContext ctx);

    @Nonnull
    @Override
    Object visitScalarFunctionName(@Nonnull RelationalParser.ScalarFunctionNameContext ctx);

    @Nonnull
    @Override
    Expressions visitFunctionArgs(@Nonnull RelationalParser.FunctionArgsContext ctx);

    @Nonnull
    @Override
    Object visitFunctionArg(@Nonnull RelationalParser.FunctionArgContext ctx);

    @Nonnull
    @Override
    Expression visitIsExpression(@Nonnull RelationalParser.IsExpressionContext ctx);

    @Nonnull
    @Override
    Expression visitNotExpression(@Nonnull RelationalParser.NotExpressionContext ctx);

    @Nonnull
    @Override
    Expression visitLikePredicate(@Nonnull RelationalParser.LikePredicateContext ctx);

    @Nonnull
    @Override
    Expression visitLogicalExpression(@Nonnull RelationalParser.LogicalExpressionContext ctx);

    @Nonnull
    @Override
    Object visitPredicateExpression(@Nonnull RelationalParser.PredicateExpressionContext ctx);

    @Nonnull
    @Override
    Object visitExpressionAtomPredicate(@Nonnull RelationalParser.ExpressionAtomPredicateContext ctx);

    @Nonnull
    @Override
    Expression visitBinaryComparisonPredicate(@Nonnull RelationalParser.BinaryComparisonPredicateContext ctx);

    @Nonnull
    @Override
    Expression visitInPredicate(@Nonnull RelationalParser.InPredicateContext ctx);

    @Nonnull
    @Override
    Expression visitInList(@Nonnull RelationalParser.InListContext ctx);

    @Nonnull
    @Override
    Object visitJsonExpressionAtom(@Nonnull RelationalParser.JsonExpressionAtomContext ctx);

    @Nonnull
    @Override
    Expression visitSubqueryExpressionAtom(@Nonnull RelationalParser.SubqueryExpressionAtomContext ctx);

    @Nonnull
    @Override
    Object visitConstantExpressionAtom(@Nonnull RelationalParser.ConstantExpressionAtomContext ctx);

    @Nonnull
    @Override
    Expression visitFunctionCallExpressionAtom(@Nonnull RelationalParser.FunctionCallExpressionAtomContext ctx);

    @Nonnull
    @Override
    Object visitFullColumnNameExpressionAtom(@Nonnull RelationalParser.FullColumnNameExpressionAtomContext ctx);

    @Nonnull
    @Override
    Expression visitBitExpressionAtom(@Nonnull RelationalParser.BitExpressionAtomContext ctx);

    @Nonnull
    @Override
    Expression visitPreparedStatementParameterAtom(@Nonnull RelationalParser.PreparedStatementParameterAtomContext ctx);

    @Nonnull
    @Override
    Object visitRecordConstructorExpressionAtom(@Nonnull RelationalParser.RecordConstructorExpressionAtomContext ctx);

    @Nonnull
    @Override
    Object visitArrayConstructorExpressionAtom(@Nonnull RelationalParser.ArrayConstructorExpressionAtomContext ctx);

    @Nonnull
    @Override
    Expression visitMathExpressionAtom(@Nonnull RelationalParser.MathExpressionAtomContext ctx);

    @Nonnull
    @Override
    Expression visitExistsExpressionAtom(@Nonnull RelationalParser.ExistsExpressionAtomContext ctx);

    @Nonnull
    @Override
    Object visitIntervalExpressionAtom(@Nonnull RelationalParser.IntervalExpressionAtomContext ctx);

    @Nonnull
    @Override
    Expression visitPreparedStatementParameter(@Nonnull RelationalParser.PreparedStatementParameterContext ctx);

    @Nonnull
    @Override
    Object visitUnaryOperator(@Nonnull RelationalParser.UnaryOperatorContext ctx);

    @Nonnull
    @Override
    Object visitComparisonOperator(@Nonnull RelationalParser.ComparisonOperatorContext ctx);

    @Nonnull
    @Override
    Object visitLogicalOperator(@Nonnull RelationalParser.LogicalOperatorContext ctx);

    @Nonnull
    @Override
    Object visitBitOperator(@Nonnull RelationalParser.BitOperatorContext ctx);

    @Nonnull
    @Override
    Object visitMathOperator(@Nonnull RelationalParser.MathOperatorContext ctx);

    @Nonnull
    @Override
    Object visitJsonOperator(@Nonnull RelationalParser.JsonOperatorContext ctx);

    @Nonnull
    @Override
    Object visitCharsetNameBase(@Nonnull RelationalParser.CharsetNameBaseContext ctx);

    @Nonnull
    @Override
    Object visitIntervalTypeBase(@Nonnull RelationalParser.IntervalTypeBaseContext ctx);

    @Nonnull
    @Override
    Object visitKeywordsCanBeId(@Nonnull RelationalParser.KeywordsCanBeIdContext ctx);

    @Nonnull
    @Override
    Object visitFunctionNameBase(@Nonnull RelationalParser.FunctionNameBaseContext ctx);

    @Nonnull
    @Override
    Object visitExecuteContinuationStatement(@Nonnull RelationalParser.ExecuteContinuationStatementContext ctx);
}
