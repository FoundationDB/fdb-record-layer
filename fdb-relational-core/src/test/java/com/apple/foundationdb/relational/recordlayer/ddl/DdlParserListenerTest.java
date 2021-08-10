/*
 * DdlParserListenerTest.java
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

import com.apple.foundationdb.relational.RelationalLexer;
import com.apple.foundationdb.relational.RelationalParser;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;

public class DdlParserListenerTest {
    /* ****************************************************************************************************************/
    /*CREATE SCHEMA TEMPLATE tests*/
    @Test
    void canParseCreateSchemaTemplateWithoutProperties(){
        String correctTemplateName = "Test";
        AtomicBoolean visited = new AtomicBoolean(false);
        ConstantActionFactory testFactory = new AbstractConstantActionFactory(){
            @Override
            public @Nonnull
            ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate templateName, @Nonnull Options templateProperties) {
                visited.set(true);
                Assertions.assertEquals(correctTemplateName,templateName,"Incorrect template name");
                Assertions.assertNotNull(templateProperties,"Null passed to nonnull properties field!");
                Assertions.assertEquals(0,templateProperties.size(),"Extra Properties were created!");
                return super.getCreateSchemaTemplateConstantAction(templateName, templateProperties);
            }
        };

        String test = "CREATE SCHEMA TEMPLATE "+ correctTemplateName+";";

        parseDdlTest(test,testFactory);
        Assertions.assertTrue(visited.get(),"Did not visit listener!");
    }

    @Test
    void canParseCreateSchemaTemplateWithProperties(){
        String correctTemplateName = "Test";
        Options checkProps = new Options().withOption(new OperationOption<>("test1","value1"));
        AtomicBoolean visited = new AtomicBoolean(false);
        ConstantActionFactory testFactory = new AbstractConstantActionFactory(){
            @Override
            public @Nonnull
            ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate templateName, @Nonnull Options templateProperties) {
                visited.set(true);
                Assertions.assertEquals(correctTemplateName,templateName,"Incorrect template name");
                Assertions.assertNotNull(templateProperties,"Null passed to nonnull properties field!");
                Assertions.assertEquals(checkProps,templateProperties,"Extra Properties were created!");
                return super.getCreateSchemaTemplateConstantAction(templateName, templateProperties);
            }
        };

        String test = "CREATE SCHEMA TEMPLATE "+ correctTemplateName+" WITH PROPERTIES ('test1'='value1');";

        parseDdlTest(test,testFactory);
        Assertions.assertTrue(visited.get(),"Did not visit listener!");
    }


    private void parseDdlTest(String ddlCmd, ConstantActionFactory actionFactory) {
        RelationalLexer lexer = new RelationalLexer(CharStreams.fromString(ddlCmd));
        RelationalParser parser = new RelationalParser(new CommonTokenStream(lexer));

        final RelationalParser.StatementsContext statements = parser.statements();
        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(new DdlParserListener(actionFactory),statements);
    }
}
