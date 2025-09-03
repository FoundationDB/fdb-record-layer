/*
 * LaunchSQLLine.java
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

import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

/**
 * Convenience class for launching sqlline so can run it under the debugger.
 * Be aware that the intellij terminal is dodgy. See
 * <a href="https://github.com/julianhyde/sqlline/blob/main/HOWTO.md#running-sqlline-inside-intellij-ideas-console-on-windows">sqlline in intellij</a>.
 * To run statements, you will need to copy and paste your statements into the intellij terminal window.
 */
@ExcludeFromJacocoGeneratedReport // Test utility only.
public class LaunchSQLLine {
    public static void main(String[] args/*Ignored*/) throws Exception {
        // Read [SQLLINE-80] to see why maxWidth must be set
        com.apple.foundationdb.relational.cli.sqlline.RelationalSQLLine.main(new String []{
                "-u", "jdbc:embed:/__SYS",
                "--maxWidth=160",
        });
    }
}
