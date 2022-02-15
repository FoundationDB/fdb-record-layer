/*
 * SetSchemaCommand.java
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

/**
 * Command that sets the current schema, subsequent CLIs commands such as {@link InsertInto} will
 * refer to this schema when looking up the table to insert the data into.
 */
@CommandLine.Command(name = "setschema", description = "Sets the current connection schema")
public class SetSchemaCommand extends CommandWithConnection {

    @CommandLine.Parameters(index = "0", description = "name of the schema")
    private String schemaName;

    public SetSchemaCommand(DbState dbState) {
        super(dbState);
    }

    @Override
    public void callInternal() throws Exception {
        dbState.getConnection().setSchema(schemaName);
    }
}
