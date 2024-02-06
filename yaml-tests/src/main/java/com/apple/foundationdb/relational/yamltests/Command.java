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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.generated.schemainstance.SchemaInstanceOuterClass;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class Command {

    static final String COMMAND_LOAD_SCHEMA_TEMPLATE = "load schema template";
    static final String COMMAND_SET_SCHEMA_STATE = "set schema state";
    static final String COMMAND_QUERY = "query";

    private static final Logger logger = LogManager.getLogger(Command.class);

    static Command resolve(String commandString) {
        if (COMMAND_LOAD_SCHEMA_TEMPLATE.equals(commandString)) {
            return new Command() {
                @Override
                public void invoke(@Nonnull List<?> region, @Nonnull RelationalConnection connection,
                                   @Nonnull YamlRunner.YamlExecutionContext executionContext) throws SQLException, RelationalException {
                    final var loadCommandString = Matchers.string(Matchers.firstEntry(Matchers.first(region, "schema template"), "schema template").getValue(), "schema template");
                    logger.debug("⏳ Loading template '{}'", loadCommandString);
                    // current connection should be __SYS/catalog
                    // save schema template
                    StoreCatalog backingCatalog = ((EmbeddedRelationalConnection) connection).getBackingCatalog();
                    RecordLayerMetadataOperationsFactory metadataOperationsFactory = new RecordLayerMetadataOperationsFactory.Builder()
                            .setBaseKeySpace(RelationalKeyspaceProvider.getKeySpace())
                            .setRlConfig(RecordLayerConfig.getDefault())
                            .setStoreCatalog(backingCatalog)
                            .build();
                    ((EmbeddedRelationalConnection) connection).ensureTransactionActive();
                    try (Transaction transaction = ((EmbeddedRelationalConnection) connection).getTransaction()) {
                        metadataOperationsFactory.getCreateSchemaTemplateConstantAction(CommandUtil.fromProto(loadCommandString), Options.NONE).execute(transaction);
                        connection.commit();
                    }
                }
            };
        } else if (COMMAND_SET_SCHEMA_STATE.equals(commandString)) {
            return new Command() {
                @Override
                public void invoke(@Nonnull List<?> region, @Nonnull RelationalConnection connection,
                                   @Nonnull YamlRunner.YamlExecutionContext executionContext) throws SQLException, RelationalException {
                    final var loadCommandString = Matchers.string(Matchers.firstEntry(Matchers.first(region, "set schema state"), "set schema state").getValue(), "set schema state");
                    logger.debug("⏳ Setting schema state '{}'", loadCommandString);
                    // current connection should be __SYS/catalog
                    StoreCatalog backingCatalog = ((EmbeddedRelationalConnection) connection).getBackingCatalog();
                    SchemaInstanceOuterClass.SchemaInstance schemaInstance = CommandUtil.fromJson(loadCommandString);
                    RecordLayerConfig rlConfig = new RecordLayerConfig.RecordLayerConfigBuilder()
                            .setIndexStateMap(CommandUtil.fromIndexStateProto(schemaInstance.getIndexStatesMap()))
                            .setFormatVersion(schemaInstance.getStoreInfo().getFormatVersion())
                            .setUserVersionChecker((oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(schemaInstance.getStoreInfo().getUserVersion()))
                            .build();
                    RecordLayerMetadataOperationsFactory metadataOperationsFactory = new RecordLayerMetadataOperationsFactory.Builder()
                            .setBaseKeySpace(RelationalKeyspaceProvider.getKeySpace())
                            .setRlConfig(rlConfig)
                            .setStoreCatalog(backingCatalog)
                            .build();

                    ((EmbeddedRelationalConnection) connection).ensureTransactionActive();
                    try (Transaction transaction = ((EmbeddedRelationalConnection) connection).getTransaction()) {
                        metadataOperationsFactory.getSetStoreStateConstantAction(URI.create(schemaInstance.getDatabaseId()), schemaInstance.getName()).execute(transaction);
                        connection.commit();
                    }
                }
            };
        } else if (COMMAND_QUERY.equals(commandString)) {
            return new QueryCommand(false);
        } else {
            Assert.failUnchecked(String.format("‼️ could not find command '%s'", commandString));
            return null;
        }
    }

    public abstract void invoke(@Nonnull final List<?> region, @Nonnull final RelationalConnection connection,
                                @Nonnull YamlRunner.YamlExecutionContext executionContext) throws SQLException, RelationalException;
}
