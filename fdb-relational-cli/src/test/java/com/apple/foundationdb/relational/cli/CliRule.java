/*
 * CliRule.java
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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

import java.io.PrintWriter;
import java.io.StringWriter;

public class CliRule implements BeforeEachCallback, AfterEachCallback {
    private DbState dbState;

    private CommandLine cmd;

    private StringWriter output;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        dbState = new DbState();
        CliCommands cli = new CliCommands();
        CommandFactory commandFactory = new CommandFactory(dbState);
        PicocliCommands.PicocliCommandsFactory factory = new PicocliCommands.PicocliCommandsFactory(commandFactory);
        cmd = new CommandLine(cli, factory);
        output = new StringWriter();
        cmd.setOut(new PrintWriter(output));
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        dbState.getEngine().deregisterDriver();
    }

    public CommandLine getCmd() {
        return cmd;
    }

    public DbState getDbState() {
        return dbState;
    }

    public String getOutput() {
        return output.toString();
    }
}
