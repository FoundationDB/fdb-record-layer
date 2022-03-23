/*
 * CliManager.java
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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

import java.io.PrintWriter;

import javax.annotation.Nullable;

import static org.fusesource.jansi.Ansi.ansi;

public class CliManager implements AutoCloseable {
    private final DbState dbState;

    private final CommandLine cmd;
    private final CliCommands cliCommands = new CliCommands();
    private final PicocliCommands.PicocliCommandsFactory factory;

    public CliManager(@Nullable PrintWriter output, @Nullable PrintWriter errorOutput) throws RelationalException {
        this.dbState = new DbState();
        // set up picocli commands

        CommandFactory commandFactory = new CommandFactory(dbState);
        factory = new PicocliCommands.PicocliCommandsFactory(commandFactory);
        cmd = new CommandLine(cliCommands, factory);
        CommandLine.IExecutionExceptionHandler errorHandler = (ex, commandLine, parseResult) -> {
            commandLine.getErr().println(ansi().render("@|red " + ex.getMessage() + "|@"));
            if (dbState.isPrintStacktrace()) {
                ex.printStackTrace(commandLine.getErr());
            }
            commandLine.usage(commandLine.getErr());
            return commandLine.getCommandSpec().exitCodeOnExecutionException();
        };
        cmd.setExecutionExceptionHandler(errorHandler);

        if (output != null) {
            cmd.setOut(output);
        }
        if (errorOutput != null) {
            cmd.setErr(errorOutput);
        }
    }

    public DbState getDbState() {
        return dbState;
    }

    public CommandLine getCommandLine() {
        return cmd;
    }

    @ExcludeFromJacocoGeneratedReport
    public void setReader(LineReader reader) {
        cliCommands.setReader(reader);
    }

    @ExcludeFromJacocoGeneratedReport
    public void setTerminal(Terminal terminal) {
        factory.setTerminal(terminal);
    }

    @Override
    public void close() throws Exception {
        dbState.getEngine().deregisterDriver();
        dbState.close();
    }
}
