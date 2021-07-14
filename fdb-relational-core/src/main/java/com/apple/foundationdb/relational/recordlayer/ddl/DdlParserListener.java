/*
 * DdlParserListener.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.RelationalParser;
import com.apple.foundationdb.relational.RelationalParserBaseListener;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class DdlParserListener extends RelationalParserBaseListener {
    private final ConstantActionFactory constantActionFactory;
    private final List<ConstantAction> parsedActions = new LinkedList<>();

    public DdlParserListener(ConstantActionFactory constantActionFactory) {
        this.constantActionFactory = constantActionFactory;
    }

    public List<ConstantAction> getParsedActions(){
        return Collections.unmodifiableList(parsedActions);
    }

    @Override
    public void enterCreateTemplateStatement(RelationalParser.CreateTemplateStatementContext ctx) {
        final RelationalParser.IdentifierContext templateName = ctx.identifier();

        String name = templateName.Identifier().getText();
        Properties templateProperties = parseProperties(ctx.dbProperties());

        parsedActions.add(constantActionFactory.getCreateSchemaTemplateConstantAction(name,templateProperties));
    }

    @Override
    public void enterCreateTableStatement(RelationalParser.CreateTableStatementContext ctx) {
        final RelationalParser.IdentifierContext tNameCtx = ctx.identifier();

        String name = tNameCtx.Identifier().getText();
        final List<RelationalParser.TableColumnContext> tableColumnCtxs = ctx.tableColumnList().tableColumn();
        List<ColumnDescriptor> columns = tableColumnCtxs.stream().map(tableColumnContext -> {
            String colName = tableColumnContext.identifier().Identifier().getText();
            String dataTypeStr = tableColumnContext.dataType().getText();
            DataType dataType = DataType.valueOf(dataTypeStr);
            boolean isRepeated = tableColumnContext.KW_REPEATED()!=null;
            boolean isPk = tableColumnContext.KW_PRIMARYKEY()!=null;

            return new ColumnDescriptor(colName,dataType,isRepeated,isPk);
        }).collect(Collectors.toList());

        Properties dbProps = parseProperties(ctx.dbProperties());

        parsedActions.add(constantActionFactory.getCreateTableConstantAction(name,columns,dbProps));
    }

    @Override
    public void enterCreateValueIndexStatement(RelationalParser.CreateValueIndexStatementContext ctx) {
        final RelationalParser.IdentifierContext idxNameCtx = ctx.identifier(0);
        final RelationalParser.IdentifierContext tableNameCtx = ctx.identifier(1);

        final List<RelationalParser.ValueIndexColumnContext> valueIndexColumnContexts = ctx.valueIndexColumn();
        final List<ValueIndexColumnDescriptor> columns = valueIndexColumnContexts.stream()
                .map(valueIndexColumnContext -> new ValueIndexColumnDescriptor(valueIndexColumnContext.getText())).collect(Collectors.toList());
        Properties props = parseProperties(ctx.dbProperties());

        parsedActions.add(constantActionFactory.getCreateValueIndexConstantAction(idxNameCtx.getText(),tableNameCtx.getText(),columns,props));
    }

    private Properties parseProperties(RelationalParser.DbPropertiesContext dbPropertiesContext) {
        Properties dbProps = new Properties();
        if (dbPropertiesContext != null) {
            final RelationalParser.DbPropertiesListContext dbPropsListCtx = dbPropertiesContext.dbPropertiesList();
            if (dbPropsListCtx != null) {
                final List<RelationalParser.KeyValuePropertyContext> keyValuePropertyContexts = dbPropsListCtx.keyValueProperty();
                for (RelationalParser.KeyValuePropertyContext kvpc : keyValuePropertyContexts) {
                    final String key = kvpc.StringLiteral(0).getText();
                    final String value = kvpc.StringLiteral(1).getText();
                    //remove the antlr-required quotes around each string
                    dbProps.put(key.substring(1, key.length() - 1), value.substring(1, value.length() - 1));
                }
            }
        }
        return dbProps;
    }
}
