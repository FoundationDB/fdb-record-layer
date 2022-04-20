/*
 * DdlCommand.java
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

import com.apple.foundationdb.relational.api.ddl.DdlConnection;
import com.apple.foundationdb.relational.api.ddl.DdlStatement;

import picocli.CommandLine;

import java.util.List;

@CommandLine.Command(name = "ddl", description = "Executes a DDL statement")
public class DdlCommand extends Command {
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    Args args;

    private static class Args {
        /*
         * This is setup to make it easier for us to specify a script file at a later date
         */
        @CommandLine.Option(names = {"-c", "--command"}, description = "A ddl statement to execute directly")
        String command;
    }

    DdlCommand(DbState dbState) {
        super(dbState, List.of());
    }

    @Override
    protected void callInternal() throws Exception {
        String cmd = args.command;
        try (DdlConnection ddlConn = dbState.getEngine().getDdlConnection(); DdlStatement statement = ddlConn.createStatement()) {
            statement.execute(cmd);
            ddlConn.commit();
        }
    }
}
