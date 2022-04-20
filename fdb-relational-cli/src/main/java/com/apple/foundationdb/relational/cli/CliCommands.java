/*
 * CliCommands.java
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

import org.jline.reader.LineReader;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

import java.io.PrintWriter;

/**
 * Placeholder for all commands used to setup the interactive terminal.
 */
@ExcludeFromJacocoGeneratedReport //excluded because it doesn't do anything really
@CommandLine.Command(name = "",
        subcommands = {
                ConnectCommand.class,
                DdlCommand.class,
                DisconnectCommand.class,
                SetSchemaCommand.class,
                InsertInto.class,
                ConfCommand.class,
                PicocliCommands.ClearScreen.class,
                ListSchemas.class,
                CommandLine.HelpCommand.class})
class CliCommands implements Runnable {
    PrintWriter out;

    CliCommands() {
    }

    public void setReader(LineReader reader) {
        out = reader.getTerminal().writer();
    }

    @Override
    public void run() {
        out.println(new CommandLine(this).getUsageMessage());
    }
}
