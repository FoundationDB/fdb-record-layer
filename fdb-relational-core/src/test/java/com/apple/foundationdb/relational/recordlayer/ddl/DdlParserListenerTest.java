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
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class DdlParserListenerTest {
    @Test
    void canParseCreateValueIndexWithoutProperties(){
        String correctIndexName = "test_idx";
        String correctTableName = "test_table";
        AtomicBoolean visited = new AtomicBoolean(false);
        List<ValueIndexColumnDescriptor> correctColumns = Arrays.asList(
                new ValueIndexColumnDescriptor("col1"),
                new ValueIndexColumnDescriptor("col2"),
                new ValueIndexColumnDescriptor("col3")
        );
        ConstantActionFactory testFactory = new AbstractConstantActionFactory(){
            @Nonnull
            @Override
            public ConstantAction getCreateValueIndexConstantAction(@Nonnull String indexName,@Nonnull String tableName,@Nonnull List<ValueIndexColumnDescriptor> columns, @Nonnull Properties tableProps) {
                visited.set(true);
                Assertions.assertEquals(correctIndexName,indexName,"Incorrect index name");
                Assertions.assertEquals(correctTableName,tableName,"Incorrect table name");
                Assertions.assertNotNull(columns,"Null passed to nonnull columns field");
                Assertions.assertEquals(correctColumns,columns,"Incorrect columns");
                Assertions.assertNotNull(tableProps,"Null passed to nonnull properties field!");
                Assertions.assertEquals(0,tableProps.size(),"Extra Properties were created!");
                return super.getCreateValueIndexConstantAction(indexName,tableName, columns,tableProps);
            }
        };

        parseDdlTest("CREATE VALUE INDEX "+correctIndexName+" ON "+correctTableName+"(col1, col2, col3)",testFactory);
        Assertions.assertTrue(visited.get(),"Did not visit listener!");
    }

    @Test
    void canParseCreateTableWithoutProperties(){
        String correctTableName = "test_table";
        AtomicBoolean visited = new AtomicBoolean(false);
        List<ColumnDescriptor> correctColumns = Arrays.asList(
                new ColumnDescriptor("col1", DataType.LONG, false, false),
                new ColumnDescriptor("col2", DataType.DOUBLE, true, false),
                new ColumnDescriptor("col3", DataType.STRING, false, true)
        );
        ConstantActionFactory testFactory = new AbstractConstantActionFactory(){
            @Nonnull
            @Override
            public ConstantAction getCreateTableConstantAction(@Nonnull String tableName,@Nonnull List<ColumnDescriptor> columns, @Nonnull Properties tableProps) {
                visited.set(true);
                Assertions.assertEquals(correctTableName,tableName,"Incorrect table name");
                Assertions.assertNotNull(columns,"Null passed to nonnull columns field");
                Assertions.assertEquals(correctColumns,columns,"Incorrect columns");
                Assertions.assertNotNull(tableProps,"Null passed to nonnull properties field!");
                Assertions.assertEquals(0,tableProps.size(),"Extra Properties were created!");
                return super.getCreateTableConstantAction(tableName, columns,tableProps);
            }
        };

        parseDdlTest("CREATE TABLE "+correctTableName+"(col1 LONG, col2 DOUBLE REPEATED, col3 STRING PRIMARY KEY);",testFactory);
        Assertions.assertTrue(visited.get(),"Did not visit listener!");
    }

    @Test
    void canParseCreateTableWithProperties(){
        String correctTableName = "test_table";
        Properties correctProps = new Properties();
        correctProps.put("testTableProp","testTableValue");

        List<ColumnDescriptor> correctColumns = Arrays.asList(
                new ColumnDescriptor("col1", DataType.LONG, false, false),
                new ColumnDescriptor("col2", DataType.DOUBLE, true, false),
                new ColumnDescriptor("col3", DataType.STRING, false, true)
        );

        AtomicBoolean visited = new AtomicBoolean(false);
        ConstantActionFactory testFactory = new AbstractConstantActionFactory(){
            @Nonnull
            @Override
            public ConstantAction getCreateTableConstantAction(@Nonnull String tableName, @Nonnull List<ColumnDescriptor> columns, @Nonnull Properties tableProps) {
                visited.set(true);
                Assertions.assertEquals(correctTableName,tableName,"Incorrect table name");
                Assertions.assertNotNull(tableProps,"Null passed to nonnull properties field!");
                Assertions.assertEquals(correctColumns,columns,"Incorrect columns returned");
                Assertions.assertEquals(correctProps,tableProps,"Incorrect Properties were created!");
                return super.getCreateTableConstantAction(tableName,columns, tableProps);
            }
        };

        parseDdlTest("CREATE TABLE "+correctTableName+"(col1 LONG, col2 DOUBLE REPEATED, col3 STRING PRIMARY KEY) WITH PROPERTIES ('testTableProp'='testTableValue');",testFactory);
        Assertions.assertTrue(visited.get(),"Did not visit listener!");
    }
    /* ****************************************************************************************************************/
    /*CREATE SCHEMA TEMPLATE tests*/
    @Test
    void canParseCreateSchemaTemplateWithoutProperties(){
        String correctTemplateName = "Test";
        AtomicBoolean visited = new AtomicBoolean(false);
        ConstantActionFactory testFactory = new AbstractConstantActionFactory(){
            @Override
            public @Nonnull ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull String templateName, @Nonnull Properties templateProperties) {
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
        Properties checkProps = new Properties();
        checkProps.put("test1","value1");
        AtomicBoolean visited = new AtomicBoolean(false);
        ConstantActionFactory testFactory = new AbstractConstantActionFactory(){
            @Override
            public @Nonnull ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull String templateName, @Nonnull Properties templateProperties) {
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
