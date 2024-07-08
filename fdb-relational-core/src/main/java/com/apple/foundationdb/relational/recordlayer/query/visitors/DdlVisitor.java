/*
 * DdlVisitor.java
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

import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.generated.RelationalParser;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.Identifier;
import com.apple.foundationdb.relational.recordlayer.query.IndexGenerator;
import com.apple.foundationdb.relational.recordlayer.query.ProceduralPlan;
import com.apple.foundationdb.relational.recordlayer.query.SemanticAnalyzer;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    // TODO: remove
    @Override
    @Nonnull
    public DataType visitColumnType(@Nonnull RelationalParser.ColumnTypeContext ctx) {
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        if (ctx.customType != null) {
            final var columnType = visitUid(ctx.customType);
            return semanticAnalyzer.lookupType(columnType, false, false, metadataBuilder::findType);
        }
        return visitPrimitiveType(ctx.primitiveType());
    }

    // TODO: remove
    @Override
    @Nonnull
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
    @Override
    @Nonnull
    public RecordLayerColumn visitColumnDefinition(@Nonnull RelationalParser.ColumnDefinitionContext ctx) {
        final var columnId = visitUid(ctx.colName);
        final var isRepeated = ctx.ARRAY() != null;
        final var isNullable = ctx.columnConstraint() != null ? (Boolean) ctx.columnConstraint().accept(this) : true;
        containsNullableArray = containsNullableArray || (isRepeated && isNullable);
        final var columnTypeId = ctx.columnType().customType != null ? visitUid(ctx.columnType().customType) : Identifier.of(ctx.columnType().getText());
        final var semanticAnalyzer = getDelegate().getSemanticAnalyzer();
        final var columnType = semanticAnalyzer.lookupType(columnTypeId, isNullable, isRepeated, metadataBuilder::findType);
        return RecordLayerColumn.newBuilder().setName(columnId.getName()).setDataType(columnType).build();
    }

    @Override
    @Nonnull
    public RecordLayerTable visitTableDefinition(@Nonnull RelationalParser.TableDefinitionContext ctx) {
        final var tableId = visitUid(ctx.uid());
        final var columns = ctx.columnDefinition().stream().map(this::visitColumnDefinition).collect(ImmutableList.toImmutableList());
        final var primaryKeyParts = ctx.primaryKeyDefinition().fullId().stream().map(this::visitFullId)
                .map(Identifier::fullyQualifiedName).collect(ImmutableList.toImmutableList());
        final var tableBuilder = RecordLayerTable.newBuilder(metadataBuilder.isIntermingleTables())
                .setName(tableId.getName())
                .addColumns(columns);
        primaryKeyParts.forEach(tableBuilder::addPrimaryKeyPart);
        return tableBuilder.build();
    }

    @Override
    @Nonnull
    public RecordLayerTable visitStructDefinition(@Nonnull RelationalParser.StructDefinitionContext ctx) {
        final var structId = visitUid(ctx.uid());
        final var columns = ctx.columnDefinition().stream().map(this::visitColumnDefinition).collect(ImmutableList.toImmutableList());
        final var structBuilder = RecordLayerTable.newBuilder(metadataBuilder.isIntermingleTables())
                .setName(structId.getName())
                .addColumns(columns);
        return structBuilder.build();
    }

    @Override
    @Nonnull
    public RecordLayerIndex visitIndexDefinition(@Nonnull RelationalParser.IndexDefinitionContext ctx) {
        final var indexId = visitUid(ctx.indexName);

        final var ddlCatalog = metadataBuilder.build();
        // parse the index SQL query using the newly constructed metadata.
        getDelegate().replaceCatalog(ddlCatalog);
        final var viewPlan = getDelegate().getPlanGenerationContext().withDisabledLiteralProcessing(() ->
                visitQuerySpecification(ctx.querySpecification()).getQuantifier().getRangesOver().get());

        final var isUnique = ctx.UNIQUE() != null;
        final var generator = IndexGenerator.from(viewPlan);
        final var table = metadataBuilder.findTable(generator.getRecordTypeName());
        Assert.thatUnchecked(viewPlan instanceof LogicalSortExpression, ErrorCode.INVALID_COLUMN_REFERENCE, "Cannot create index and order by an expression that is not present in the projection list");
        return generator.generate(indexId.getName(), isUnique, table.getType(), containsNullableArray);
    }

    @Override
    @Nonnull
    public DataType.Named visitEnumDefinition(@Nonnull RelationalParser.EnumDefinitionContext ctx) {
        final var enumId = visitUid(ctx.uid());

        // (yhatem) we have control over the ENUM values' numbers.
        final List<DataType.EnumType.EnumValue> enumValues = new ArrayList<>(ctx.STRING_LITERAL().size());
        for (int i = 0; i < ctx.STRING_LITERAL().size(); i++) {
            enumValues.add(DataType.EnumType.EnumValue.of(Assert.notNullUnchecked(getDelegate().normalizeString(ctx.STRING_LITERAL(i).getText())), i));
        }
        return DataType.EnumType.from(enumId.getName(), enumValues, false);
    }

    @Override
    @Nonnull
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
        for (final var templateClause : ctx.templateClause()) {
            if (templateClause.enumDefinition() != null) {
                metadataBuilder.addAuxiliaryType(visitEnumDefinition(templateClause.enumDefinition()));
            } else if (templateClause.structDefinition() != null) {
                structClauses.add(templateClause.structDefinition());
            } else if (templateClause.tableDefinition() != null) {
                tableClauses.add(templateClause.tableDefinition());
            } else {
                Assert.thatUnchecked(templateClause.indexDefinition() != null);
                indexClauses.add(templateClause.indexDefinition());
            }
        }
        structClauses.build().stream().map(this::visitStructDefinition).map(RecordLayerTable::getDatatype).forEach(metadataBuilder::addAuxiliaryType);
        tableClauses.build().stream().map(this::visitTableDefinition).forEach(metadataBuilder::addTable);
        final var indexes = indexClauses.build().stream().map(this::visitIndexDefinition).collect(ImmutableList.toImmutableList());
        for (final var index : indexes) {
            final var table = metadataBuilder.extractTable(index.getTableName());
            final var tableWithIndex = RecordLayerTable.Builder.from(table).addIndex(index).build();
            metadataBuilder.addTable(tableWithIndex);
        }
        return ProceduralPlan.of(metadataOperationsFactory.getCreateSchemaTemplateConstantAction(metadataBuilder.build(), Options.NONE));
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateSchemaStatement(@Nonnull RelationalParser.CreateSchemaStatementContext ctx) {
        final var schemaId = visitUid(ctx.schemaId().path().uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        final var templateId = visitUid(ctx.schemaTemplateId().uid());
        return ProceduralPlan.of(metadataOperationsFactory.getCreateSchemaConstantAction(dbAndSchema.getLeft().orElse(dbUri),
                dbAndSchema.getRight(), templateId.getName(), Options.NONE));
    }

    @Override
    @Nonnull
    public ProceduralPlan visitCreateDatabaseStatement(@Nonnull RelationalParser.CreateDatabaseStatementContext ctx) {
        final var databaseId = visitUid(ctx.path().uid());
        SemanticAnalyzer.validateDatabaseUri(databaseId);
        return ProceduralPlan.of(metadataOperationsFactory.getCreateDatabaseConstantAction(URI.create(databaseId.getName()), Options.NONE));
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropDatabaseStatement(@Nonnull RelationalParser.DropDatabaseStatementContext ctx) {
        final var databaseId = visitUid(ctx.path().uid());
        SemanticAnalyzer.validateDatabaseUri(databaseId);
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return  ProceduralPlan.of(metadataOperationsFactory.getDropDatabaseConstantAction(URI.create(databaseId.getName()), throwIfDoesNotExist, Options.NONE));
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropSchemaStatement(@Nonnull RelationalParser.DropSchemaStatementContext ctx) {
        final var schemaId = visitUid(ctx.uid());
        final var dbAndSchema = SemanticAnalyzer.parseSchemaIdentifier(schemaId);
        Assert.thatUnchecked(dbAndSchema.getLeft().isPresent(), ErrorCode.UNKNOWN_DATABASE, () -> String.format("invalid database identifier in '%s'", ctx.uid().getText()));
        return ProceduralPlan.of(metadataOperationsFactory.getDropSchemaConstantAction(dbAndSchema.getLeft().get(), dbAndSchema.getRight(), Options.NONE));
    }

    @Override
    @Nonnull
    public ProceduralPlan visitDropSchemaTemplateStatement(@Nonnull RelationalParser.DropSchemaTemplateStatementContext ctx) {
        final var schemaTemplateId = visitUid(ctx.uid());
        boolean throwIfDoesNotExist = ctx.ifExists() == null;
        return ProceduralPlan.of(metadataOperationsFactory.getDropSchemaTemplateConstantAction(schemaTemplateId.getName(),
                throwIfDoesNotExist, Options.NONE));
    }

    // TODO: remove
    @Override
    @Nonnull
    public Boolean visitNullColumnConstraint(@Nonnull RelationalParser.NullColumnConstraintContext ctx) {
        return ctx.nullNotnull().NOT() == null;
    }
}
