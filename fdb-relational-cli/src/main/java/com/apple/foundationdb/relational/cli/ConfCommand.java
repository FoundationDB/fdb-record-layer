/*
 * ConfCommand.java
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

import picocli.CommandLine;

import java.util.List;

/**
 * Command for common configuration of both the CLI and the database itself.
 */
@CommandLine.Command(name = "config", description = "Control the environment configuration")
public class ConfCommand extends Command {

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    Args args;

    public ConfCommand(DbState dbState) {
        super(dbState, List.of());
    }

    @Override
    public void callInternal() {
        if (args.bt != null) {
            dbState.setPrintStacktrace(args.bt);
        } else if (args.prettyPrint != null) {
            dbState.setPrettyPrint(args.prettyPrint);
        } else if (args.delimiter != null) {
            if (dbState.isPrettyPrint()) {
                throw new IllegalStateException("cannot set delimiter with pretty-print on");
            } else {
                dbState.setDelimiter(args.delimiter);
            }
        } else if (args.headers != null) {
            dbState.setDisplayHeaders(args.headers);
        }
    }

    static class Args {
        @CommandLine.Option(names = "--bt", description = "print exception backtrace", negatable = true)
        private Boolean bt;

        @CommandLine.Option(names = "--pretty-print", description = "pretty-print result set", negatable = true)
        private Boolean prettyPrint;

        @CommandLine.Option(names = "--delimiter", description = "record delimiter to output (required --no-pretty-print)")
        private String delimiter;

        @CommandLine.Option(names = "--headers", description = "show / hide result set column headers", negatable = true)
        private Boolean headers;
    }
}
