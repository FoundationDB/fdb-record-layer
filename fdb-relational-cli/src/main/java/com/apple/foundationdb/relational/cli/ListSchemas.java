/*
 * ListSchemas.java
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

import com.apple.foundationdb.relational.api.RelationalResultSet;

import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/**
 * Command that lists all available schemas of the database we're connected to.
 */
@CommandLine.Command(name = "listschemas", description = "Lists all the schemas in the database")
public class ListSchemas extends CommandWithConnection {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    public ListSchemas(DbState dbState) {
        super(dbState);
    }

    @Override
    public void callInternal() throws Exception {
        try (RelationalResultSet schemas = dbState.getConnection().getMetaData().getSchemas()) {
            List<List<String>> results = new ArrayList<>();
            while (schemas.next()) {
                results.add(List.of(schemas.getString("TABLE_SCHEM")));
            }
            Utils.tabulate(spec.commandLine().getOut(), dbState.isPrettyPrint(), dbState.getDelimiter(), dbState.isDisplayHeaders(), results, new String[]{"schemas"});
        }
    }
}
