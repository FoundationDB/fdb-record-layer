/*
 * Command.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.cli.DbState;
import com.apple.foundationdb.relational.cli.DbStateCommandFactory;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;

import javax.annotation.Nonnull;

public abstract class Command {

    private static final Logger LOG = LogManager.getLogger(Command.class);

    static void debug(@Nonnull final String message) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(message);
        }
    }

    static void error(@Nonnull final String message) {
        if (LOG.isErrorEnabled()) {
            LOG.error(message);
        }
    }

    static Command resolve(String commandString) {
        if ("connect".equals(commandString)) {
            return new Command() {
                @Override
                public void invoke(@Nonnull List<?> region, @Nonnull DbStateCommandFactory factory, @Nonnull DbState dbState) throws Exception {
                    final var uri = Matchers.string(Matchers.firstEntry(Matchers.first(region, "connect"), "connect").getValue(), "connect");
                    debug(String.format("connecting to '%s'", uri));
                    final var connectionFun = factory.getConnectCommand(URI.create(Matchers.notNull(uri, "connection URI")));
                    connectionFun.call();
                    debug(String.format("connected to '%s'", uri));
                }
            };
        } else if ("insert".equals(commandString)) {
            return new Command() {
                @Override
                public void invoke(@Nonnull List<?> region, @Nonnull DbStateCommandFactory factory, @Nonnull DbState dbState) throws Exception {
                    final var tableEntry = Matchers.firstEntry(Matchers.second(region), "table name");
                    Matchers.matches(Matchers.notNull(Matchers.string(Matchers.notNull(tableEntry, "table name").getKey(), "table name"), "table name"), "table");
                    final var tableName = Matchers.notNull(Matchers.string(Matchers.notNull(tableEntry, "table name").getValue(), "table name"), "table name");
                    final var connection = Matchers.notNull(dbState.getConnection(), "database connection");
                    try (var statement = connection.createStatement()) {
                        debug("parsing YAML input into PB Message(s)");
                        final var yamlData = Matchers.notNull(Matchers.firstEntry(Matchers.first(region), "insert data").getValue(), "insert data");
                        final DynamicMessageBuilder tableRowBuilder = Matchers.notNull(statement.getDataBuilder(tableName), String.format("table '%s' message builder", tableName));
                        final var dataList = Generators.yamlToDynamicMessage(yamlData, tableRowBuilder);
                        if (dataList.isEmpty()) {
                            debug(String.format("⚠️ parsed 0 rows, skipping insert into '%s'", tableName));
                            return;
                        }
                        debug(String.format("inserting %d row(s) in '%s'", dataList.size(), tableName));
                        statement.executeInsert(tableName, dataList); // todo: affected rows.
                        debug(String.format("inserting %d row(s) in '%s'", dataList.size(), tableName));
                    }
                }
            };
        } else if ("query".equals(commandString)) {
            return new CommandQuery();
        } else {
            Assert.failUnchecked(String.format("‼️ could not find command '%s'", commandString));
            return null;
        }
    }

    public abstract void invoke(@Nonnull final List<?> region,
                                @Nonnull final DbStateCommandFactory factory,
                                @Nonnull final DbState dbState) throws Exception;
}
