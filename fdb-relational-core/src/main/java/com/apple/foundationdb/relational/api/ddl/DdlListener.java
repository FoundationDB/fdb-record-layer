/*
 * DdlListener.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.TableInfo;
import com.apple.foundationdb.relational.api.catalog.TypeInfo;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.generated.DdlParser;
import com.apple.foundationdb.relational.generated.DdlParserBaseListener;

import com.google.protobuf.DescriptorProtos;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DdlListener extends DdlParserBaseListener {
    private SchemaTemplateBuilder templateBuilder;
    private CustomTypeBuilder typeBuilder;
    private TableBuilder tableBuilder;

    private final ConstantActionFactory constantActionFactory;
    private final DdlQueryFactory queryFactory;
    private final Queue<DdlPreparedAction<?>> parsedActions = new LinkedList<>();
    private final URI rootUri;

    private final Supplier<URI> databaseUriSupplier;

    public DdlListener(URI rootUri,
                       ConstantActionFactory constantActionFactory,
                       DdlQueryFactory ddlQueryFactory,
                       Supplier<URI> databaseUriSupplier) {
        this.rootUri = rootUri;
        this.constantActionFactory = constantActionFactory;
        this.queryFactory = ddlQueryFactory;
        this.databaseUriSupplier = databaseUriSupplier;
    }

    public Queue<DdlPreparedAction<?>> getParsedActions() {
        return parsedActions;
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        super.exitEveryRule(ctx);
    }

    @Override
    public void enterStatement(DdlParser.StatementContext ctx) {
        super.enterStatement(ctx);
    }

    @Override
    public void exitStatement(DdlParser.StatementContext ctx) {
        super.exitStatement(ctx);
    }

    @Override
    public void enterSchemaTemplateDef(DdlParser.SchemaTemplateDefContext ctx) {
        templateBuilder = new SchemaTemplateBuilder(ctx.identifier().getText());
    }

    @Override
    public void exitSchemaTemplateDef(DdlParser.SchemaTemplateDefContext ctx) {
        if (ctx.exception != null) {
            ErrorCode code = ErrorCode.SYNTAX_ERROR;
            String message = ctx.exception.getMessage();
            if (ctx.exception instanceof InputMismatchException) {
                final Token offendingToken = ctx.exception.getOffendingToken();
                if ("}".equals(offendingToken.getText())) {
                    //this occurs when the SchemaTemplate is empty, which is a different errorCode
                    code = ErrorCode.INVALID_SCHEMA_TEMPLATE;
                    message = "No Types or Tables defined in Schema Template";
                } else {
                    message = "Unexpected token at position " + offendingToken.getTokenIndex() + ": " + offendingToken.getText();
                }
            } else if (message == null) {
                message = "Unexpected syntax error";
            }
            throw new ParseCancellationException(new RelationalException(message, code));
        }
        try {
            parsedActions.add(constantActionFactory.getCreateSchemaTemplateConstantAction(templateBuilder.build(), Options.create()));
        } catch (RelationalException e) {
            throw new ParseCancellationException(e);
        }
        templateBuilder = null;
    }

    @Override
    public void enterCreateStatement(DdlParser.CreateStatementContext ctx) {
        super.enterCreateStatement(ctx);
    }

    @Override
    public void exitCreateStatement(DdlParser.CreateStatementContext ctx) {
        if (ctx.exception != null) {
            String message = ctx.exception.getMessage();
            throw new ParseCancellationException(new RelationalException(message, ErrorCode.SYNTAX_ERROR));
        }
        //TODO(bfines) this feels weird--want to put all the creates together?
        if (ctx.databaseIdentifier() != null) {
            parsedActions.add(constantActionFactory.getCreateDatabaseConstantAction(URI.create(ctx.databaseIdentifier().Path().getText()), Options.create()));
        }
    }

    @Override
    public void exitSchemaDef(DdlParser.SchemaDefContext ctx) {
        SchemaDef def = parseSchema(ctx.schemaIdentifier());
        String templateId = ctx.templateIdentifier().identifier().getText();

        parsedActions.add(constantActionFactory.getCreateSchemaConstantAction(def.db, def.schemaName, templateId, Options.create()));
    }

    @Override
    public void enterStructDef(DdlParser.StructDefContext ctx) {
        if (ctx.getChildCount() <= 0) {
            return; // empty string no biggie
        }
        if (ctx.getChild(0).equals(ctx.KW_STRUCT())) {
            typeBuilder = new CustomTypeBuilder(templateBuilder.customTypes, ctx.identifier().getText());
        } else {
            tableBuilder = new TableBuilder(templateBuilder.customTypes, ctx.identifier().getText());
        }
    }

    @Override
    public void exitValueIndexDef(DdlParser.ValueIndexDefContext ctx) {
        //first two identifiers are the index and table name, the rest are columns
        final List<DdlParser.IdentifierContext> allIdentifiers = ctx.identifier();
        final DdlParser.IdentifierContext indexNameNode = allIdentifiers.get(0);
        if (indexNameNode.exception != null) {
            throw new ParseCancellationException(indexNameNode.exception);
        }
        String indexName = indexNameNode.getText();

        final DdlParser.IdentifierContext tableNameNode = allIdentifiers.get(1);
        if (indexNameNode.exception != null) {
            throw new ParseCancellationException(tableNameNode.exception);
        }
        String tableName = tableNameNode.getText();

        List<String> indexedFields = new LinkedList<>();
        List<String> includeFields = new LinkedList<>();

        boolean inIndexedFields = false;
        boolean inIncludedFields = false;
        int passedCommaCount = 0;
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (ctx.getChild(i).equals(ctx.LPAREN(0))) {
                inIndexedFields = true;
            } else if (ctx.getChild(i).equals(ctx.RPAREN(0))) {
                inIndexedFields = false;
            } else if (ctx.getChild(i).equals(ctx.LPAREN(1))) {
                inIncludedFields = true;
            } else if (ctx.getChild(i).equals(ctx.RPAREN(1))) {
                inIncludedFields = false;
            } else if (ctx.getChild(i).equals(ctx.COMMA(passedCommaCount))) {
                passedCommaCount++;
            } else {
                //we have an actual field!
                if (inIndexedFields) {
                    indexedFields.add(ctx.getChild(i).getText());
                } else if (inIncludedFields) {
                    includeFields.add(ctx.getChild(i).getText());
                }
            }
        }

        //RecordLayer wants _lower case_ values for the type string
        IndexBuilder indexBuilder = new IndexBuilder(indexName)
                .setType("value")
                .indexFields(indexedFields)
                .includeFields(includeFields);
        try {
            templateBuilder.registerIndex(tableName, indexBuilder);
        } catch (RelationalException e) {
            throw new ParseCancellationException(e);
        }
    }

    @Override
    public void exitStructDef(DdlParser.StructDefContext ctx) {
        if (ctx.exception != null) {
            String message = ctx.exception.getMessage();
            throw new ParseCancellationException(new RelationalException(message, ErrorCode.SYNTAX_ERROR));
        }
        try {
            if (typeBuilder != null) {
                templateBuilder.registerType(typeBuilder.getTypeName(), typeBuilder.buildDescriptor());
                typeBuilder = null;
            } else if (tableBuilder != null) {
                templateBuilder.registerTable(tableBuilder.getTypeName(), tableBuilder);
                tableBuilder = null;
            }

        } catch (RelationalException e) {
            throw new ParseCancellationException(e);
        }
    }

    @Override
    public void exitPrimaryKeyDef(DdlParser.PrimaryKeyDefContext ctx) {
        if (tableBuilder == null) {
            throw new ParseCancellationException(new RelationalException("Primary key defined on a non-table", ErrorCode.SYNTAX_ERROR));
        }
        //look for the record type at the beginning
        final List<DdlParser.IdentifierContext> columnNodes = ctx.identifier();
        for (DdlParser.IdentifierContext columnNode : columnNodes) {
            tableBuilder.primaryKeyColumns.add(columnNode.getText());
        }
        if (columnNodes.isEmpty() && ctx.KW_RECORD_TYPE() != null) {
            //make sure you add a record type, even if the pk columns are empty
            tableBuilder.forceRecordTypeKey();
        }

        super.exitPrimaryKeyDef(ctx);
    }

    @Override
    public void enterColumnDef(DdlParser.ColumnDefContext ctx) {
        try {
            final String columnLabel = ctx.identifier(0).getText();
            DescriptorProtos.FieldDescriptorProto.Builder fdProto = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(columnLabel);
            int pos = 1;

            ParseTree typeNode = ctx.getChild(pos);
            fdProto = fdProto.setType(getColumnType(typeNode.getText()));
            ParseTree arrNode = ctx.getChild(pos + 1);
            if (arrNode != null && arrNode.equals(ctx.KW_ARRAY())) {
                fdProto.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
            }

            String messageType = null;
            if (fdProto.getType() == DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE) {
                messageType = typeNode.getText();
            }

            if (typeBuilder != null) {
                //we are defining a custom type
                typeBuilder.registerField(fdProto, messageType);
            } else if (tableBuilder != null) {
                tableBuilder.registerField(fdProto, messageType);
            }
        } catch (RelationalException ve) {
            throw new RuntimeException(ve);
        }
    }

    @Override
    public void exitDropStatement(DdlParser.DropStatementContext ctx) {
        if (ctx.databaseIdentifier() != null) {
            parsedActions.add(constantActionFactory.getDropDatabaseConstantAction(URI.create(ctx.databaseIdentifier().Path().getText()), Options.create()));
        } else if (ctx.schemaOrTemplateIdentifier() != null) {
            final DdlParser.SchemaOrTemplateIdentifierContext stIdentifierCtx = ctx.schemaOrTemplateIdentifier();
            if (stIdentifierCtx.schemaIdentifier() != null) {
                SchemaDef parsedSchema = parseSchema(stIdentifierCtx.schemaIdentifier());
                parsedActions.add(constantActionFactory.getDropSchemaConstantAction(parsedSchema.db, parsedSchema.schemaName, Options.create()));
            } else {
                //drop schema template
                parsedActions.add(constantActionFactory.getDropSchemaTemplateConstantAction(stIdentifierCtx.templateIdentifier().identifier().getText(), Options.create()));
            }
        }
    }

    @Override
    public void exitDatabaseIdentifier(DdlParser.DatabaseIdentifierContext ctx) {
        if (ctx.Path() == null) {
            throw new ParseCancellationException(new RelationalException("Invalid Path", ErrorCode.INVALID_PATH));
        }
    }

    @Override
    public void exitShowDatabases(DdlParser.ShowDatabasesContext ctx) {
        if (ctx.exception != null) {
            String message = ctx.exception.getMessage();
            throw new ParseCancellationException(new RelationalException(message, ErrorCode.SYNTAX_ERROR));
        }

        final TerminalNode path = ctx.Path();
        URI dbPath;
        if (path == null) {
            dbPath = rootUri;
        } else {
            dbPath = URI.create(path.getText());
        }

        parsedActions.add(queryFactory.getListDatabasesQueryAction(dbPath));
    }

    @Override
    public void exitShowTemplates(DdlParser.ShowTemplatesContext ctx) {

        parsedActions.add(queryFactory.getListSchemaTemplatesQueryAction());
    }

    @Override
    public void exitShowStatement(DdlParser.ShowStatementContext ctx) {
        if (ctx.exception != null) {
            String message = ctx.exception.getMessage();
            throw new ParseCancellationException(new RelationalException(message, ErrorCode.SYNTAX_ERROR));
        }
    }

    @Override
    public void exitTemplateIdentifier(DdlParser.TemplateIdentifierContext ctx) {
        final DdlParser.IdentifierContext identifier = ctx.identifier();
        if (identifier == null || identifier.isEmpty() || identifier.getText().equals("<missing Identifier>")) {
            throw new ParseCancellationException(new RelationalException("No Template Id specified", ErrorCode.INVALID_PARAMETER));
        }
    }

    @Override
    public void exitDescribeStatement(DdlParser.DescribeStatementContext ctx) {
        if (ctx.exception != null) {
            String message = ctx.exception.getMessage();
            throw new ParseCancellationException(new RelationalException(message, ErrorCode.SYNTAX_ERROR));
        }
        final DdlParser.SchemaOrTemplateIdentifierContext identifierCtx = ctx.schemaOrTemplateIdentifier();
        if (identifierCtx != null) {
            //it's DESCRIBE SCHEMA or DESCRIBE SCHEMA TEMPLATE
            if (identifierCtx.schemaIdentifier() != null) {
                SchemaDef schemaDef = parseSchema(identifierCtx.schemaIdentifier());
                parsedActions.add(queryFactory.getDescribeSchemaQueryAction(schemaDef.db, schemaDef.schemaName));
            } else if (identifierCtx.templateIdentifier() != null) {
                //DESCRIBE TEMPLATE
                final DdlParser.IdentifierContext templateId = identifierCtx.templateIdentifier().identifier();
                if (templateId != null) {
                    parsedActions.add(queryFactory.getDescribeSchemaTemplateQueryAction(templateId.getText()));
                } else {
                    //shouldn't happen until we add some other describes
                    throw new ParseCancellationException(new RelationalException("DESCRIBE must be followed with either SCHEMA or SCHEMA TEMPLATE", ErrorCode.SYNTAX_ERROR));

                }
            } else {
                //shouldn't happen until we add some other describes
                throw new ParseCancellationException(new RelationalException("DESCRIBE must be followed with either SCHEMA or SCHEMA TEMPLATE", ErrorCode.SYNTAX_ERROR));
            }
        } else {
            //shouldn't happen until we add some other describes
            throw new ParseCancellationException(new RelationalException("DESCRIBE must be followed with either SCHEMA or SCHEMA TEMPLATE", ErrorCode.SYNTAX_ERROR));
        }
    }

    @Override
    public void exitSchemaIdentifier(DdlParser.SchemaIdentifierContext ctx) {
        SchemaDef schema = parseSchema(ctx);
        if (schema.db == null) {
            throw new ParseCancellationException(new RelationalException("No Database specified", ErrorCode.INVALID_PARAMETER));
        }
        if (schema.schemaName == null) {
            throw new ParseCancellationException(new RelationalException("No Schema specified", ErrorCode.INVALID_PARAMETER));
        }
    }

    static class SchemaDef {
        private final URI db;
        private final String schemaName;

        public SchemaDef(URI db, String schemaName) {
            this.db = db;
            this.schemaName = schemaName;
        }
    }

    private SchemaDef parseSchema(DdlParser.SchemaIdentifierContext ctx) {
        String schemaName;
        URI dbUri;
        if (ctx.Path() != null) {
            URI path = URI.create(ctx.Path().getText());
            //the schema name is always the leaf
            final int schemaTerminator = path.getPath().lastIndexOf("/");
            schemaName = path.getPath().substring(schemaTerminator + 1);
            dbUri = URI.create(path.getPath().substring(0, schemaTerminator));
        } else {
            schemaName = ctx.identifier().getText();
            dbUri = databaseUriSupplier.get();
        }

        return new SchemaDef(dbUri, schemaName);
    }

    // todo: refactor and move to its own package
    public static class SchemaTemplateBuilder {
        private final String templateName;
        private final Map<String, TypeInfo> customTypes = new HashMap<>();
        private final Map<String, TableBuilder> tables = new LinkedHashMap<>();

        public SchemaTemplateBuilder(String templateName) {
            this.templateName = templateName;
        }

        public SchemaTemplate build() throws RelationalException {
            Set<TypeInfo> types = Collections.newSetFromMap(new IdentityHashMap<>());
            types.addAll(customTypes.values());
            Set<TableInfo> tbls = new LinkedIdentitySet<>();
            for (TableBuilder tb : tables.values()) {
                tbls.add(tb.buildTable());
            }
            return new SchemaTemplateDescriptor(templateName, tbls, types, 1);
        }

        public void registerType(String typeName, DescriptorProtos.DescriptorProto type) throws RelationalException {
            if (customTypes.containsKey(typeName)) {
                throw new RelationalException("Type <" + typeName + "> has already been defined", ErrorCode.INTERNAL_ERROR);
            }
            customTypes.put(typeName, new TypeInfo(type));
        }

        public void registerTable(String tableName, TableBuilder tableDef) throws RelationalException {
            if (tables.containsKey(tableName)) {
                throw new RelationalException("Table <" + tableName + "> already exists", ErrorCode.SYNTAX_ERROR);
            }

            tables.put(tableName, tableDef);
        }

        public void registerIndex(String tableName, IndexBuilder indexDef) throws RelationalException {
            final TableBuilder tableBuilder = tables.get(tableName);
            if (tableBuilder == null) {
                throw new RelationalException("Unknown Table " + tableName, ErrorCode.UNKNOWN_TYPE);
            }
            tableBuilder.addIndex(indexDef.build());
        }
    }

    // todo: refactor and move to its own package
    public static class CustomTypeBuilder {
        private final Map<String, TypeInfo> customTypes;
        private final String typeName;
        private final DescriptorProtos.DescriptorProto.Builder typeBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        private int columnId = 1;

        public CustomTypeBuilder(Map<String, TypeInfo> customTypes, String typeName) {
            this.customTypes = customTypes;
            this.typeName = typeName;
            this.typeBuilder.setName(typeName);
        }

        public void registerField(DescriptorProtos.FieldDescriptorProto.Builder field, String messageType) throws RelationalException {
            if (messageType != null) {
                //verify that the message type has been defined
                TypeInfo typeInfo = customTypes.get(messageType);
                if (typeInfo == null) {
                    throw new RelationalException("Unknown Message type: < " + messageType + ">", ErrorCode.SYNTAX_ERROR);
                }
                field.setTypeName(messageType);
            }
            field.setNumber(columnId);
            columnId++;
            this.typeBuilder.addField(field);
        }

        public DescriptorProtos.DescriptorProto buildDescriptor() {
            return typeBuilder.build();
        }

        public String getTypeName() {
            return typeName;
        }
    }

    // todo: refactor and move to its own package
    public static class TableBuilder extends CustomTypeBuilder {
        //TODO(bfines) we'll need to add features for different types of primary keys here
        private List<String> primaryKeyColumns = new LinkedList<>();
        private List<CatalogData.Index> indexes = new LinkedList<>();
        private boolean forceRecordTypeKey;

        public TableBuilder(Map<String, TypeInfo> customTypes, String typeName) {
            super(customTypes, typeName);
        }

        public TableInfo buildTable() throws RelationalException {
            if (primaryKeyColumns.isEmpty()) {
                if (!forceRecordTypeKey) {
                    throw new RelationalException("Cannot define a table without a primary key", ErrorCode.INVALID_TABLE_DEFINITION);
                }
            }
            List<KeyExpression> fieldKeys = primaryKeyColumns.stream().map(Key.Expressions::field).collect(Collectors.toList());
            fieldKeys.add(0, Key.Expressions.recordType());
            //TODO(bfines) move this logic to somewhere more carefully dependency managed
            KeyExpression ke = fieldKeys.size() == 1 ? fieldKeys.get(0) : Key.Expressions.concat(fieldKeys);

            CatalogData.Table tbl = CatalogData.Table.newBuilder().setName(getTypeName())
                    .setPrimaryKey(ke.toKeyExpression().toByteString())
                    .addAllIndexes(indexes)
                    .build();
            return new TableInfo(getTypeName(), tbl, buildDescriptor());
        }

        public TableBuilder addIndex(CatalogData.Index index) throws RelationalException {
            for (CatalogData.Index idx : indexes) {
                if (idx.getName().equalsIgnoreCase(index.getName())) {
                    throw new RelationalException(String.format("index %s is already defined in this Template", index.getName()), ErrorCode.INDEX_EXISTS);
                }
            }
            this.indexes.add(index);
            return this;
        }

        public TableBuilder addPrimaryKeyColumns(final List<String> primaryKeyColumns) {
            this.primaryKeyColumns = primaryKeyColumns;
            return this;
        }

        public void forceRecordTypeKey() {
            this.forceRecordTypeKey = true;
        }
    }

    // todo: refactor and move to its own package
    public static class IndexBuilder {
        private RecordMetaDataProto.Index.Builder indexBuilder = RecordMetaDataProto.Index.newBuilder();
        private List<String> includedFields;
        private List<String> indexedFields;

        public IndexBuilder(String name) {
            indexBuilder = this.indexBuilder.setName(name);
        }

        public IndexBuilder setType(String type) {
            indexBuilder.setType(type);
            return this;
        }

        public IndexBuilder indexFields(List<String> fields) {
            this.indexedFields = fields;
            return this;
        }

        public IndexBuilder includeFields(List<String> includeFields) {
            this.includedFields = includeFields;
            return this;
        }

        public CatalogData.Index build() {
            List<KeyExpression> fieldExpressions = indexedFields.stream()
                    .map(Key.Expressions::field).collect(Collectors.toList());
            List<KeyExpression> includeExprs = includedFields.stream()
                    .map(Key.Expressions::field).collect(Collectors.toList());
            fieldExpressions.add(0, Key.Expressions.recordType());
            KeyExpression rootExpression;
            if (includedFields.isEmpty()) {
                rootExpression = Key.Expressions.concat(fieldExpressions);
            } else {
                List<KeyExpression> allFields = new ArrayList<>(fieldExpressions.size() + includeExprs.size());
                allFields.addAll(fieldExpressions);
                allFields.addAll(includeExprs);
                rootExpression = Key.Expressions.keyWithValue(Key.Expressions.concat(allFields), fieldExpressions.size());
            }
            indexBuilder.setRootExpression(rootExpression.toKeyExpression());
            return CatalogData.Index.newBuilder().setName(indexBuilder.getName())
                    .setIndexDef(indexBuilder.build().toByteString())
                    .build();
        }

    }

    private DescriptorProtos.FieldDescriptorProto.Type getColumnType(String text) throws RelationalException {
        switch (text.toUpperCase(Locale.ROOT)) {
            case "STRING":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
            case "INT64":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            case "DOUBLE":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
            case "BOOLEAN":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
            case "BYTES":
                return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
            default:
                //look in the type definitions to see if we know what it is
                if (this.templateBuilder.customTypes.containsKey(text)) {
                    return DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;
                } else {
                    throw new RelationalException("Invalid column type <" + text + ">", ErrorCode.SYNTAX_ERROR);
                }
        }
    }

}
