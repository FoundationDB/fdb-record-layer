/*
 * InsertInto.java
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
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;

import com.google.protobuf.Message;
import picocli.CommandLine;

/**
 * A command that inserts a records into a table.
 */
@CommandLine.Command(name = "insertinto", description = "Inserts a new record into a table")
public class InsertInto extends CommandWithConnectionAndSchema {

    @CommandLine.Parameters(index = "0", description = "name of the table, must be fully-qualified")
    private String table;

    @CommandLine.Parameters(index = "1", description = "record to insert into json format")
    private String json;

    public InsertInto(DbState dbState) {
        super(dbState);
    }

    @Override
    public void callInternal() throws Exception {
        try (RelationalStatement s = dbState.getConnection().createStatement()) {
            Iterable<Message> data = Utils.jsonToDynamicMessage(json, s.getDataBuilder(table));

            s.executeInsert(table.substring(Math.max(table.lastIndexOf('.'), table.lastIndexOf('$')) + 1), data, Options.create());
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        }
    }
}
