/*
 * SQLLineTest.java
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

package com.apple.foundationdb.relational.cli;

import com.google.common.base.Charsets;
import org.junit.jupiter.api.Test;
import sqlline.SqlLine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SQLline doesn't do the intellij terminal very well.
 * See this old doc on it that is outdated but basic point stands:
 * https://github.com/julianhyde/sqlline/blob/main/HOWTO.md#running-sqlline-inside-intellij-ideas-console-on-windows
 */
public class SQLLineTest {
    /**
     * Simple test that launches sqlline, connects to our jdbc:embed driver and then run simple
     * 'select * from databases'.
     * @throws IOException If failure capturing stdout when sqlline runs.
     */
    @Test
    public void runSqlline() throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ByteArrayOutputStream err = new ByteArrayOutputStream();
                ByteArrayInputStream in = new ByteArrayInputStream(new byte[0])) {
            final SqlLine sqlLine = new SqlLine();
            sqlLine.setOutputStream(out);
            sqlLine.setErrorStream(err);
            final SqlLine.Status status = sqlLine.begin(new String[] {
                    "-ac", "com.apple.foundationdb.relational.cli.sqlline.Customize",
                    // We don't need the below because we add our command over in our
                    // sqlline Customize class.
                    // "-ch", "sqlline.PlannerDebuggerCommandHandler",
                    "-u", "jdbc:relational:///__SYS?schema=CATALOG",
                    "-d", "com.apple.foundationdb.relational.jdbc.JDBCRelationalDriver",
                    "--maxWidth=257",
                    "-e", "select * from databases;"
            }, in, false);
            try {
                assertEquals(status, SqlLine.Status.OK);

                final String output = out.toString(Charsets.UTF_8);
                assertThat(output).contains("DATABASE_ID")
                        .contains("/__SYS");
            } catch (AssertionError e) {
                System.out.println(err.toString(StandardCharsets.UTF_8));
                System.out.println(out.toString(StandardCharsets.UTF_8));
                throw e;
            }
        }
    }
}
