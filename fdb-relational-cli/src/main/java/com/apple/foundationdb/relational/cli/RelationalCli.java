/*
 * RelationalCli.java
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

import org.fusesource.jansi.AnsiConsole;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.keymap.KeyMap;
import org.jline.reader.Binding;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.Reference;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

/**
 * Main entry point of Relational CLI; an interactive shell that offers a set of commands allowing
 * the user to interact with Relational and the underlying FDB database.
 *
 * The CLI currently only supports in-memory catalog, once the Java process is ended, all the metadata is gone.
 * For this reason (and some other ones), consider using this CLI only for experimenting with Relational and playing
 * around with it.
 */
//excluded because it's almost entirely configuring jline and running it
@ExcludeFromJacocoGeneratedReport
@SuppressWarnings({"PMD.AvoidCatchingThrowable", "PMD.AvoidPrintStackTrace", "PMD.EmptyCatchBlock"}) // justification: interactive shell.
public final class RelationalCli {

    public static void main(String[] args) {
        AnsiConsole.systemInstall();
        try {
            Supplier<Path> workDir = () -> Paths.get(System.getProperty("user.dir"));
            // set up JLine built-in commands
            Builtins builtins = new Builtins(workDir, null, null);
            builtins.rename(Builtins.Command.TTOP, "top");

            try (CliManager cliManager = new CliManager(null, null)) {
                RelationalCommands picocliCommands = new RelationalCommands(cliManager.getCommandLine());
                Parser parser = new DefaultParser();
                try (Terminal terminal = TerminalBuilder.builder().build()) {
                    SystemRegistry systemRegistry = new SystemRegistryImpl(parser, terminal, workDir, null);
                    systemRegistry.setCommandRegistries(builtins, picocliCommands);
                    systemRegistry.register("help", picocliCommands);

                    LineReader reader = LineReaderBuilder.builder()
                            .terminal(terminal)
                            .completer(systemRegistry.completer())
                            .parser(parser)
                            .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                            .build();
                    builtins.setLineReader(reader);
                    cliManager.setReader(reader);
                    cliManager.setTerminal(terminal);
                    KeyMap<Binding> keyMap = reader.getKeyMaps().get("main");
                    keyMap.bind(new Reference("tailtip-toggle"), KeyMap.alt("s"));

                    String prompt = "prompt> ";
                    String rightPrompt = null;

                    // start the shell and process input until the user quits with Ctrl-D
                    String line;
                    while (true) {
                        try {
                            systemRegistry.cleanUp();
                            line = reader.readLine(prompt, rightPrompt, (MaskingCallback) null, null);
                            if (line.isEmpty() || systemRegistry.hasCommand(parser.parse(line, 0).word())) { // todo: improve checks
                                systemRegistry.execute(line);
                            } else {
                                // parse SQL statement
                                new QueryCommand(cliManager.getDbState(), line, reader.getTerminal().writer()).call();
                            }
                        } catch (UserInterruptException e) {
                            // Ignore
                        } catch (EndOfFileException e) {
                            return;
                        } catch (Exception e) {
                            systemRegistry.trace(e);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            AnsiConsole.systemUninstall();
        }
    }

    @ExcludeFromJacocoGeneratedReport
    static class RelationalCommands extends PicocliCommands {
        public RelationalCommands(CommandLine cmd) {
            super(cmd);
        }
    }

    private RelationalCli() {
    }

}
