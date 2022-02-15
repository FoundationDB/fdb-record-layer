/*
 * ConnectCommand.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;

import picocli.CommandLine;

import java.net.URI;
import java.util.List;

/**
 * Command that establishes a connection to the database.
 */
@CommandLine.Command(name = "connect", description = "Connects to a database")
public class ConnectCommand extends Command {

    @CommandLine.Parameters(index = "0", description = "database URI")
    private URI databaseUri;

    public ConnectCommand(DbState dbState) {
        super(dbState, List.of());
    }

    @Override
    protected void callInternal() throws Exception {
        dbState.setConnection(Relational.connect(databaseUri, Options.create()));
    }
}
