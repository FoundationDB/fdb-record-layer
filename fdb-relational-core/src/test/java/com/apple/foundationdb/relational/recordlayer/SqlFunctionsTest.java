/*
 * SqlFunctionsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;

public class SqlFunctionsTest {
    @Nonnull
    private static final String SCHEMA_TEMPLATE =
            """
            create table t1(col1 bigint, col2 string, col3 integer, primary key(col1))
            create function f1 ( in a bigint, in b string )
              as select col1, col2 from t1 where col1 < a and col2 = b
            create function f2 ( in k bigint )
              as select col1, col2 from f1 (102, 'b')
            create function f3 (in x bigint, in y string)
              as select a.col1 as col1a, a.col2 as col2a, b.col1 as col1b, b.col2 as col2b from f2(x) as a, f1(x, y) as b
            create function f4 (in x bigint, in y string)
              as select col1a + col1b as s, col2a, col2b from f3(x, y)
            """;

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(CaseSensitiveDbObjectsTest.class,
            SCHEMA_TEMPLATE, new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @RegisterExtension
    @Order(4)
    public final LogAppenderRule logAppender = new LogAppenderRule("SqlFunctionsTest", PlanGenerator.class, Level.INFO);

    @BeforeEach
    void setUpDebugger() {
        Utils.enableCascadesDebugger();
    }

    @Test
    public void queryFunctionWithParameterLiterals() throws Exception {
        statement.execute("select * from f1(42, 'a') options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheMiss());
        statement.execute("select * from f1(43, 'b') options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheHit());
    }

    @Test
    public void queryNestingFunctionWithParameterLiterals() throws Exception {
        statement.execute("select * from f2(42) options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheMiss());
        statement.execute("select * from f2(43) options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheHit());
    }

    @Test
    public void queryJoinOfFunctions() throws Exception {
        statement.execute("select * from f3(103, 'b') options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheMiss());
        statement.execute("select * from f3(103, 'b') options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheHit());
    }

    @Test
    public void queryTransformedJoinOfFunctions() throws Exception {
        statement.execute("select * from f4(103, 'b') options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheMiss());
        statement.execute("select * from f4(103, 'b') options (log query)");
        Assertions.assertTrue(logAppender.lastMessageIsCacheHit());
    }
}
