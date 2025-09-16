/*
 * DdlVisitor.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Expression;
import com.apple.foundationdb.relational.recordlayer.query.Expressions;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.IndexGenerator;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.QueryParser;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.recordlayer.query.functions.CompiledSqlFunction;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ParserRuleContext;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public final class DdlVisitor extends DelegatingVisitor<BaseVisitor> {

    @Nonnull
    private final RecordLayerSchemaTemplate.Builder metadataBuilder;

    /*
    Whether the schema contains any nullable arrays.
    If it is true, we'll wrap array field in all indexes. If it is false, we won't wrap any array field in any indexes.
    It is a temporary work-around. We should wrap nullable array field in indexes. It'll be removed after this work is done.
    */
    private boolean containsNullableArray;

    @Nonnull
    private final MetadataOperationsFactory metadataOperationsFactory;

    @Nonnull
    private final URI dbUri;

    private DdlVisitor(@Nonnull BaseVisitor delegate,
                      @Nonnull MetadataOperationsFactory metadataOperationsFactory,
                      @Nonnull URI dbUri) {
        super(delegate);
        this.metadataBuilder = RecordLayerSchemaTemplate.newBuilder();
        this.metadataOperationsFactory = metadataOperationsFactory;
        this.dbUri = dbUri;
    }

    @Nonnull
    public static DdlVisitor of(@Nonnull BaseVisitor delegate,
                                @Nonnull MetadataOperationsFactory metadataOperationsFactory,
                                @Nonnull URI dbUri) {
        return new DdlVisitor(delegate, metadataOperationsFactory, dbUri);
    }

    @Nonnull
    @Override
    public DataType visitFunctionColumnType(@Nonnull final RelationalParser.FunctionColumnTypeContext ctx) {
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        if (ctx.customType != null) {
            final var columnType = visitUid(ctx.customType);
            return semanticAnalyzer.lookupType(columnType, true, false, metadataBuilder::findType);
        }
        return visitPrimitiveType(ctx.primitiveType()).withNullable(true);
    }

    // TODO: remove
    @Nonnull
    @Override
    public DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx) {
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        if (ctx.customType != null) {
            final var columnType = visitUid(ctx.customType);
            return semanticAnalyzer.lookupType(columnType, false, false, metadataBuilder::findType);
        }
        return visitPrimitiveType(ctx.primitiveType());
    }

    // TODO: remove
    @Nonnull
    @Override
    public DataType visitPrimitiveType(@Nonnull RelationalParser.PrimitiveTypeContext ctx) {
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        final var primitiveType = Identifier.of(ctx.getText());
        return semanticAnalyzer.lookupType(primitiveType, false, false, ignored -> Optional.empty());
    }

    /**
     * Visits the column definition and creates a corresponding metadata object. The column is assumed by to {@code Nullable}
     * by default. I.e., if the user mentions no nullability constraint, the type of the column becomes nullable, which
     * has implications on the internal representation of repeated types, see {@link com.apple.foundationdb.relational.util.NullableArrayUtils}
     * for more details.
     * @param ctx the parse tree.
     * @return a {@link RecordLayerTable} object that captures all the properties of the column as defined by the user.
     */
    @Nonnull
    @Override
    public RecordLayerColumn visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx) {
        final var columnId = visitUid(ctx.colName);
        final var isRepeated = ctx.ARRAY() != null;
        final var isNullable = ctx.columnConstraint() != null ? (Boolean) ctx.columnConstraint().accept(this) : true;
        // TODO: We currently do not support NOT NULL for any type other than ARRAY. This is because there is no way to
        //       specify not "nullability" at the RecordMetaData level. For ARRAY, specifying that is actually possible
        //       by means of NullableArrayWrapper. In essence, we don't actually need a wrapper per se for non-array types,
        //       but a way to represent it in RecordMetadata.
        Assert.thatUnchecked(isRepeated || isNullable, ErrorCode.UNSUPPORTED_OPERATION, "NOT NULL is only allowed for ARRAY column type");
        containsNullableArray = containsNullableArray || (isRepeated && isNullable);
        final var columnTypeId = ctx.columnType().customType != null ? visitUid(ctx.columnType().customType) : Identifier.of(ctx.columnType().getText());
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        final var columnType = semanticAnalyzer.lookupType(columnTypeId, isNullable, isRepeated, metadataBuilder::findType);
        return RecordLayerColumn.newBuilder().setName(columnId.getName()).setDataType(columnType).build();
    }

    @Nonnull
    @Override
    public RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx) {
        final var tableId = visitUid(ctx.uid());
        final var columns = ctx.columnDefinition().stream().map(this::visitColumnDefinition).collect(ImmutableList.toImmutableList());
        final var tableBuilder = RecordLayerTable.newBuilder(metadataBuilder.isIntermingleTables())
                .setName(tableId.getName())
                .addColumns(columns);
        if (ctx.primaryKeyDefinition().fullIdList() != null) {
            visitFullIdList(ctx.primaryKeyDefinition().fullIdList())
                    .stream()
                    .map(Identifier::fullyQualifiedName)
                    .forEach(tableBuilder::addPrimaryKeyPart);
        }
        return tableBuilder.build();
    }

    @Nonnull
    @Override
    public RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx) {
        final var structId = visitUid(ctx.uid());
        final var columns = ctx.columnDefinition().stream().map(this::visitColumnDefinition).collect(ImmutableList.toImmutableList());
        final var structBuilder = RecordLayerTable.newBuilder(metadataBuilder.isIntermingleTables())
                .setName(structId.getName())
                .addColumns(columns);
        return structBuilder.build();
    }

    @Nonnull
    @Override
    public RecordLayerIndex visitIndexDefinition(@Nonnull RelationalParser.IndexDefinitionContext ctx) {
        final var indexId = visitUid(ctx.indexName);

        final var ddlCatalog = metadataBuilder.build();
        // parse the index SQL query using the newly constructed metadata.
        getDelegate().replaceSchemaTemplate(ddlCatalog);
        final var viewPlan = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                Assert.castUnchecked(ctx.queryTerm().accept(this), LogicalOperator.class).getQuantifier().getRangesOver().get());

        final var useLegacyBasedExtremumEver = ctx.indexAttributes() != null && ctx.indexAttributes().indexAttribute().stream().anyMatch(attribute -> attribute.LEGACY_EXTREMUM_EVER() != null);
        final var isUnique = ctx.UNIQUE() != null;
        final var generator = IndexGenerator.from(viewPlan, useLegacyBasedExtremumEver);
        final var table = metadataBuilder.findTable(generator.getRecordTypeName());
        Assert.thatUnchecked(viewPlan instanceof LogicalSortExpression, ErrorCode.INVALID_COLUMN_REFERENCE, "Cannot create index and order by an expression that is not present in the projection list");
        return generator.generate(indexId.getName(), isUnique, table.getType(), containsNullableArray);
    }

    @Nonnull
    @Override
    public DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx) {
        final var enumId = visitUid(ctx.uid());

        // (yhatem) we have control over the ENUM values' numbers.
        final List<DataType.EnumType.EnumValue> enumValues = new ArrayList<>(ctx.STRING_LITERAL().size());
        for (int i = 0; i < ctx.STRING_LITERAL().size(); i++) {
            enumValues.add(DataType.EnumType.EnumValue.of(Assert.notNullUnchecked(getDelegate().normalizeString(ctx.STRING_LITERAL(i).getText())), i));
        }
        return DataType.EnumType.from(enumId.getName(), enumValues, false);
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaTemplateStatement(@Nonnull RelationalParser.CreateSchemaTemplateStatementContext ctx) {
        final var schemaTemplateId = visitUid(ctx.schemaTemplateId().uid());
        // schema template version will be set automatically at update operation to lastVersion + 1
        metadataBuilder.setName(schemaTemplateId.getName()).setVersion(1);
        if (ctx.optionsClause() != null) {
            for (var option : ctx.optionsClause().option()) {
                if (option.ENABLE_LONG_ROWS() != null) {
                    metadataBuilder.setEnableLongRows(option.booleanLiteral().TRUE() != null);
                } else if (option.INTERMINGLE_TABLES() != null) {
                    metadataBuilder.setIntermingleTables(option.booleanLiteral().TRUE() != null);
                } else if (option.STORE_ROW_VERSIONS() != null) {
                    metadataBuilder.setStoreRowVersions(option.booleanLiteral().TRUE() != null);
                } else {
                    Assert.failUnchecked(ErrorCode.SYNTAX_ERROR, "Encountered unknown options in schema template creation");
                }
            }
        }

        final ImmutableSet.Builder<RelationalParser.StructDefinitionContext> structClauses = ImmutableSet.builder();
        final ImmutableSet.Builder<RelationalParser.TableDefinitionContext> tableClauses = ImmutableSet.builder();
        final ImmutableSet.Builder<RelationalParser.IndexDefinitionContext> indexClauses = ImmutableSet.builder();
        final ImmutableSet.Builder<RelationalParser.SqlInvokedFunctionContext> functionClauses = ImmutableSet.builder();
        for (final var templateClause : ctx.templateClause()) {
            if (templateClause.enumDefinition() != null) {
                metadataBuilder.addAuxiliaryType(visitEnumDefinition(templateClause.enumDefinition()));
            } else if (templateClause.structDefinition() != null) {
                structClauses.add(templateClause.structDefinition());
            } else if (templateClause.tableDefinition() != null) {
                tableClauses.add(templateClause.tableDefinition());
            } else if (templateClause.sqlInvokedFunction() != null) {
                functionClauses.add(templateClause.sqlInvokedFunction());
            } else {
                Assert.thatUnchecked(templateClause.indexDefinition() != null);
                indexClauses.add(templateClause.indexDefinition());
            }
        }
        structClauses.build().stream().map(this::visitStructDefinition).map(RecordLayerTable::getDatatype).forEach(metadataBuilder::addAuxiliaryType);
        tableClauses.build().stream().map(this::visitTableDefinition).forEach(metadataBuilder::addTable);
        final var indexes = indexClauses.build().stream().map(this::visitIndexDefinition).collect(ImmutableList.toImmutableList());
        // TODO: this is currently relying on the lexical order of the function to resolve function dependencies which
        //       is limited.
        functionClauses.build().forEach(functionClause -> {
            final var invokedRoutine = getInvokedRoutineMetadata(functionClause, functionClause.functionSpecification(),
                    functionClause.routineBody(), metadataBuilder.build());
            metadataBuilder.addInvokedRoutine(invokedRoutine);
        });
        for (final var index : indexes) {
            final var table = metadataBuilder.extractTable(index.getTableName());
            final var tableWithIndex = RecordLayerTable.Builder.from(table).addIndex(index).build();
            metadataBuilder.addTable(tableWithIndex);
        }
        return ProceduralPlan.of(metadataOperationsFactory.getSaveSchemaTemplateConstantAction(metadataBuilder.build(), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx) {
        final var schemaId = visitUid(ctx.schemaId().path().uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        final var templateId = visitUid(ctx.schemaTemplateId().uid());
        return ProceduralPlan.of(metadataOperationsFactory.getCreateSchemaConstantAction(dbAndSchema.getLeft().orElse(dbUri),
                dbAndSchema.getRight(), templateId.getName(), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx) {
        final var databaseId = visitUid(ctx.path().uid());
        SemanticAnalyzer.validateDatabaseUri(databaseId);
        return ProceduralPlan.of(metadataOperationsFactory.getCreateDatabaseConstantAction(URI.create(databaseId.getName()), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx) {
        final var databaseId = visitUid(ctx.path().uid());
        SemanticAnalyzer.validateDatabaseUri(databaseId);
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return  ProceduralPlan.of(metadataOperationsFactory.getDropDatabaseConstantAction(URI.create(databaseId.getName()), throwIfDoesNotExist, Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx) {
        final var schemaId = visitUid(ctx.uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        Assert.thatUnchecked(dbAndSchema.getLeft().isPresent(), ErrorCode.UNKNOWN_DATABASE, () -> String.format(Locale.ROOT, "invalid database identifier in '%s'", ctx.uid().getText()));
        return ProceduralPlan.of(metadataOperationsFactory.getDropSchemaConstantAction(dbAndSchema.getLeft().get(), dbAndSchema.getRight(), Options.NONE));
    }

    @Nonnull
    @Override
    public ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx) {
        final var schemaTemplateId = visitUid(ctx.uid());
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return ProceduralPlan.of(metadataOperationsFactory.getDropSchemaTemplateConstantAction(schemaTemplateId.getName(),
                throwIfDoesNotExist, Options.NONE));
    }

    @Nonnull
    private RecordLayerInvokedRoutine getInvokedRoutineMetadata(@Nonnull final ParserRuleContext functionCtx,
                                                                @Nonnull final RelationalParser.FunctionSpecificationContext functionSpecCtx,
                                                                @Nonnull final RelationalParser.RoutineBodyContext bodyCtx,
                                                                @Nonnull final RecordLayerSchemaTemplate ddlCatalog) {
        // parse the index SQL query using the newly constructed metadata.
        getDelegate().replaceSchemaTemplate(ddlCatalog);

        final var isTemporary = functionCtx instanceof RelationalParser.CreateTempFunctionContext;

        // 1. get the function name.
        final var functionName = visitFullId(functionSpecCtx.schemaQualifiedRoutineName).toString();

        // 2. get the function SQL definition string.
        final var queryString = getDelegate().getPlanGenerationContext().getQuery();
        final var start = functionCtx.start.getStartIndex();
        final var stop = functionCtx.stop.getStopIndex() + 1; // inclusive.
        final var functionDefinition = (isTemporary ? "" : "CREATE ") + queryString.substring(start, stop);
        if (isTemporary) {
            getDelegate().getPlanGenerationContext().getLiteralsBuilder().setScope(functionName);
        } else {
            // prepared non-temporary SQL functions are not supported.
            QueryParser.validateNoPreparedParams(functionCtx);
        }

        // 3. visit the SQL string to generate (compile) the corresponding SQL plan.
        final Supplier<CompiledSqlFunction> compiledSqlFunction = () -> visitSqlInvokedFunction(functionSpecCtx, bodyCtx, isTemporary);

        // 4. Return it.
        return RecordLayerInvokedRoutine.newBuilder()
                .setName(functionName)
                .setDescription(functionDefinition)
                .withCompilableRoutine(ignored -> compiledSqlFunction.get())
                .setNormalizedDescription(getDelegate().getPlanGenerationContext().getCanonicalQueryString())
                .setTemporary(isTemporary)
                .build();
    }

    @Override
    public ProceduralPlan visitCreateTempFunction(@Nonnull RelationalParser.CreateTempFunctionContext ctx) {
        final var invokedRoutine = getInvokedRoutineMetadata(ctx, ctx.tempSqlInvokedFunction().functionSpecification(),
                ctx.tempSqlInvokedFunction().routineBody(), getDelegate().getSchemaTemplate());
        var throwIfExists = ctx.REPLACE() == null;
        return ProceduralPlan.of(metadataOperationsFactory.getCreateTemporaryFunctionConstantAction(getDelegate().getSchemaTemplate(),
                throwIfExists, invokedRoutine, PreparedParams.copyOf(getDelegate().getPlanGenerationContext().getPreparedParams())));
    }

    @Override
    public ProceduralPlan visitDropTempFunction(@Nonnull RelationalParser.DropTempFunctionContext ctx) {
        final var functionName = visitFullId(ctx.schemaQualifiedRoutineName).toString();
        var throwIfNotExists = ctx.IF() == null && ctx.EXISTS() == null;
        return ProceduralPlan.of(metadataOperationsFactory.getDropTemporaryFunctionConstantAction(throwIfNotExists, functionName));
    }

    @Override
    public CompiledSqlFunction visitCreateFunction(@Nonnull RelationalParser.CreateFunctionContext ctx) {
        return visitSqlInvokedFunction(ctx.sqlInvokedFunction());
    }

    @Override
    public CompiledSqlFunction visitTempSqlInvokedFunction(@Nonnull RelationalParser.TempSqlInvokedFunctionContext ctx) {
        return visitSqlInvokedFunction(ctx.functionSpecification(), ctx.routineBody(), true);
    }

    @Override
    public CompiledSqlFunction visitSqlInvokedFunction(@Nonnull RelationalParser.SqlInvokedFunctionContext ctx) {
        return visitSqlInvokedFunction(ctx.functionSpecification(), ctx.routineBody(), false);
    }

    private CompiledSqlFunction visitSqlInvokedFunction(@Nonnull final RelationalParser.FunctionSpecificationContext functionSpecCtx,
                                                        @Nonnull final RelationalParser.RoutineBodyContext bodyCtx,
                                                        boolean isTemporary) {
        // get the function name.
        final var functionName = visitFullId(functionSpecCtx.schemaQualifiedRoutineName).toString();

        // run implementation-specific validations.
        final var props = functionSpecCtx.routineCharacteristics();
        final var language = (props.languageClause() != null && props.languageClause().languageName().JAVA() != null)
                             ? InvokedRoutine.Language.JAVA
                             : InvokedRoutine.Language.SQL;
        // SQL-invoked routine 11.60, syntax rules, section 6.f.ii
        boolean isNullReturnOnNull = props.nullCallClause() != null && props.nullCallClause().RETURNS() != null;
        // ... currently we support only CALLED ON NULL INPUT (which is implicitly set if not defined).
        Assert.thatUnchecked(!isNullReturnOnNull, "only CALLED ON NULL INPUT clause is supported");
        boolean isSqlParameterStyle = props.parameterStyle() == null || props.parameterStyle().SQL() != null;
        boolean isScalar = functionSpecCtx.returnsClause() != null &&
                functionSpecCtx.returnsClause().returnsType().returnsTableType() == null;
        Assert.thatUnchecked(!isScalar, "only table functions are supported");
        Assert.thatUnchecked(isSqlParameterStyle, ErrorCode.UNSUPPORTED_OPERATION, "only sql-style parameters are supported");
        // todo: rework Java UDFs to go through this code path as well.
        Assert.thatUnchecked(language == InvokedRoutine.Language.SQL, ErrorCode.UNSUPPORTED_OPERATION,
                "only sql-language functions are supported");
        // create SQL function logical plan by visiting the function body.
        final var parameters = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                visitSqlParameterDeclarationList(functionSpecCtx.sqlParameterDeclarationList()).asNamedArguments());
        final var sqlFunctionBuilder = CompiledSqlFunction.newBuilder()
                .setName(functionName)
                .addAllParameters(parameters)
                .seal();
        final var parametersCorrelation = sqlFunctionBuilder.getParametersCorrelation();
        final LogicalOperator body;

        // the nested fragment below serves two purposes:
        // 1. avoid creating a top-level LSE unnecessarily.
        // 2. add a fake quantifier with the function parameters (if any) to resolve their references in the function body
        //    during its plan generation.
        final var fragment = getDelegate().pushPlanFragment();

        parametersCorrelation.ifPresent(quantifier -> fragment.addOperator(LogicalOperator.newUnnamedOperator(
                Expressions.fromQuantifier(quantifier), quantifier)));
        if (isTemporary) {
            body = Assert.castUnchecked(visit(bodyCtx), LogicalOperator.class);
        } else {
            body = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                    Assert.castUnchecked(visit(bodyCtx), LogicalOperator.class));
        }
        getDelegate().popPlanFragment();
        sqlFunctionBuilder.setBody(body.getQuantifier().getRangesOver().get())
                .setLiterals(getDelegate().getPlanGenerationContext().getLiterals());
        return sqlFunctionBuilder.build();
    }

    @Override
    public LogicalOperator visitStatementBody(final RelationalParser.StatementBodyContext ctx) {
        return Assert.castUnchecked(visit(ctx.queryTerm()), LogicalOperator.class);
    }

    @Override
    public LogicalOperator visitSqlReturnStatement(final RelationalParser.SqlReturnStatementContext ctx) {
        return visitReturnValue(ctx.returnValue());
    }

    @Override
    public LogicalOperator visitReturnValue(final RelationalParser.ReturnValueContext ctx) {
        Assert.failUnchecked("scalar functions are not implemented");
        return null;
    }

    @Override
    public Expressions visitSqlParameterDeclarationList(@Nonnull RelationalParser.SqlParameterDeclarationListContext ctx) {
        if (ctx.sqlParameterDeclarations() == null) {
            return Expressions.empty();
        }
        return visitSqlParameterDeclarations(ctx.sqlParameterDeclarations());
    }

    @Override
    public Expressions visitSqlParameterDeclarations(final RelationalParser.SqlParameterDeclarationsContext ctx) {
        final var parameters = Expressions.of(ctx.sqlParameterDeclaration().stream().map(this::visitSqlParameterDeclaration).collect(ImmutableList.toImmutableList()));
        final var duplicateParameters = parameters.asList().stream().flatMap(p -> p.getName().stream())
                .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) )
                .entrySet()
                .stream()
                .filter( p -> p.getValue() > 1 )
                .collect(ImmutableList.toImmutableList());
        Assert.thatUnchecked(duplicateParameters.isEmpty(), ErrorCode.INVALID_FUNCTION_DEFINITION, () ->
                "unexpected duplicate parameter(s) " + duplicateParameters.stream().map(Object::toString).collect(Collectors.joining(",")));
        return Expressions.of(ctx.sqlParameterDeclaration().stream().map(this::visitSqlParameterDeclaration).collect(ImmutableList.toImmutableList()));
    }

    @Override
    public Expression visitSqlParameterDeclaration(@Nonnull RelationalParser.SqlParameterDeclarationContext ctx) {
        Assert.thatUnchecked(ctx.sqlParameterName != null, "unnamed parameters not supported");
        final var parameterName = visitUid(ctx.sqlParameterName);
        final var parameterType = visitFunctionColumnType(ctx.parameterType);
        final var underlyingType = DataTypeUtils.toRecordLayerType(parameterType);
        Assert.thatUnchecked(parameterType.isResolved());
        Assert.thatUnchecked(ctx.parameterMode() == null || ctx.parameterMode().IN() != null, "only IN parameters are supported");
        if (ctx.DEFAULT() != null) {
            final var defaultExpression = Assert.castUnchecked(visit(ctx.parameterDefault), Expression.class);
            var defaultValue = PromoteValue.inject(defaultExpression.getUnderlying(), underlyingType);
            return Expression.of(defaultValue, parameterName);
        } else {
            return Expression.of(new ThrowsValue(underlyingType), parameterName);
        }
    }

    @Override
    public DataType visitReturnsType(@Nonnull RelationalParser.ReturnsTypeContext ctx) {
        if (ctx.returnsDataType != null) {
            return visitColumnType(ctx.returnsDataType);
        }
        throw new UnsupportedOperationException("table type is not supported");
    }

    // TODO: remove
    @Nonnull
    @Override
    public Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx) {
        return ctx.nullNotnull().NOT() == null;
    }
}
