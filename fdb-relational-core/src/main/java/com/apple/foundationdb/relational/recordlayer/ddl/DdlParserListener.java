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
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DdlParserListener extends RelationalParserBaseListener {
    private final ConstantActionFactory constantActionFactory;
    private final List<ConstantAction> parsedActions = new LinkedList<>();

    public DdlParserListener(ConstantActionFactory constantActionFactory) {
        this.constantActionFactory = constantActionFactory;
    }

    public List<ConstantAction> getParsedActions() {
        return Collections.unmodifiableList(parsedActions);
    }

    @Override
    public void enterCreateTemplateStatement(RelationalParser.CreateTemplateStatementContext ctx) {
        final RelationalParser.IdentifierContext templateName = ctx.identifier();

        String name = templateName.Identifier().getText();
        Options templateProperties = parseProperties(ctx.dbProperties());

        //TODO(bfines) implement this when we finally get the chance
//        parsedActions.add(constantActionFactory.getCreateSchemaTemplateConstantAction(name, templateProperties));
    }

    private Options parseProperties(RelationalParser.DbPropertiesContext dbPropertiesContext) {
        Options dbProps = new Options();
        if (dbPropertiesContext != null) {
            final RelationalParser.DbPropertiesListContext dbPropsListCtx = dbPropertiesContext.dbPropertiesList();
            if (dbPropsListCtx != null) {
                final List<RelationalParser.KeyValuePropertyContext> keyValuePropertyContexts = dbPropsListCtx.keyValueProperty();
                for (RelationalParser.KeyValuePropertyContext kvpc : keyValuePropertyContexts) {
                    final String key = kvpc.StringLiteral(0).getText();
                    final String value = kvpc.StringLiteral(1).getText();
                    //remove the antlr-required quotes around each string
                    dbProps.withOption(new OperationOption<>(key.substring(1, key.length() - 1), value.substring(1, value.length() - 1)));
                }
            }
        }
        return dbProps;
    }
}
