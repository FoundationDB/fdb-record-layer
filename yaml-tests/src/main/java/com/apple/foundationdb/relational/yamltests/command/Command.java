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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalogProvider;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.generated.schemainstance.SchemaInstanceOuterClass;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings({"PMD.AvoidCatchingThrowable"})
public abstract class Command {

    public static final String COMMAND_LOAD_SCHEMA_TEMPLATE = "load schema template";
    public static final String COMMAND_SET_SCHEMA_STATE = "set schema state";
    public static final String COMMAND_QUERY = "query";

    private static final Logger logger = LogManager.getLogger(Command.class);

    private final int lineNumber;
    @Nonnull
    final YamlExecutionContext executionContext;

    Command(int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        this.lineNumber = lineNumber;
        this.executionContext = executionContext;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    @Nonnull
    public static Command parse(@Nonnull Object object, @Nonnull final String blockName, @Nonnull final YamlExecutionContext executionContext) {
        final var command = Matchers.notNull(Matchers.firstEntry(Matchers.arrayList(object, "command").get(0), "command"), "command");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(command.getKey(), () -> "Invalid command key-value pair: " + command);
        final var lineNumber = linedObject.getLineNumber();
        try {
            final var key = Matchers.notNull(Matchers.string(linedObject.getObject(), "command"), "command");
            final var value = Matchers.notNull(command, "query configuration").getValue();
            switch (key) {
                case COMMAND_LOAD_SCHEMA_TEMPLATE:
                    return getLoadSchemaTemplateCommand(lineNumber, executionContext, (String) value);
                case COMMAND_SET_SCHEMA_STATE:
                    return getSetSchemaStateCommand(lineNumber, executionContext, (String) value);
                case COMMAND_QUERY:
                    return QueryCommand.parse(object, blockName, executionContext);
                default:
                    Assert.failUnchecked(String.format("Could not find command '%s'", key));
                    return null;
            }
        } catch (Exception e) {
            throw executionContext.wrapContext(e, () -> "‼️ Error parsing command at line " + lineNumber, "command", lineNumber);
        }
    }

    public final void execute(@Nonnull final RelationalConnection connection) {
        try {
            executeInternal(connection);
        } catch (Throwable e) {
            throw executionContext.wrapContext(e, () -> "‼️ Error executing command at line " + getLineNumber(), "command", getLineNumber());
        }
    }

    abstract void executeInternal(@Nonnull RelationalConnection connection) throws SQLException, RelationalException;

    private static void applyMetadataOperationEmbedded(@Nonnull EmbeddedRelationalConnection connection, @Nonnull RecordLayerConfig rlConfig, @Nonnull ApplyState applyState) throws SQLException, RelationalException {
        StoreCatalog backingCatalog = connection.getBackingCatalog();
        RecordLayerMetadataOperationsFactory metadataOperationsFactory = new RecordLayerMetadataOperationsFactory.Builder()
                .setBaseKeySpace(RelationalKeyspaceProvider.instance().getKeySpace())
                .setRlConfig(rlConfig)
                .setStoreCatalog(backingCatalog)
                .build();
        connection.setAutoCommit(false);
        connection.createNewTransaction();
        final var transaction = connection.getTransaction();
        applyState.applyMetadataOperation(metadataOperationsFactory, transaction);
        connection.commit();
        connection.setAutoCommit(true);
    }

    private static void applyMetadataOperationDirectly(@Nonnull RecordLayerConfig rlConfig, @Nonnull ApplyState applyState) throws RelationalException {
        final FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
        final RelationalKeyspaceProvider keyspaceProvider = RelationalKeyspaceProvider.instance();
        keyspaceProvider.registerDomainIfNotExists("FRL");
        KeySpace keySpace = keyspaceProvider.getKeySpace();
        StoreCatalog backingCatalog;
        try (Transaction txn = new RecordContextTransaction(fdbDb.openContext(FDBRecordContextConfig.newBuilder().build()))) {
            backingCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        }
        RecordLayerMetadataOperationsFactory metadataOperationsFactory = new RecordLayerMetadataOperationsFactory.Builder()
                .setBaseKeySpace(RelationalKeyspaceProvider.instance().getKeySpace())
                .setRlConfig(rlConfig)
                .setStoreCatalog(backingCatalog)
                .build();
        try (Transaction txn = new RecordContextTransaction(fdbDb.openContext(FDBRecordContextConfig.newBuilder().build()))) {
            applyState.applyMetadataOperation(metadataOperationsFactory, txn);
            txn.commit();
        }
    }

    private static Command getLoadSchemaTemplateCommand(int lineNumber, @Nonnull final YamlExecutionContext executionContext, @Nonnull String value) {
        return new Command(lineNumber, executionContext) {
            @Override
            public void executeInternal(@Nonnull RelationalConnection connection) throws SQLException, RelationalException {
                logger.debug("⏳ Loading template '{}'", value);
                // current connection should be __SYS/catalog
                // save schema template
                ApplyState applyState = (RecordLayerMetadataOperationsFactory factory, Transaction txn) -> {
                    factory.getCreateSchemaTemplateConstantAction(CommandUtil.fromProto(value), Options.NONE).execute(txn);
                };
                if (connection instanceof EmbeddedRelationalConnection) {
                    Command.applyMetadataOperationEmbedded((EmbeddedRelationalConnection) connection, RecordLayerConfig.getDefault(), applyState);
                } else {
                    Command.applyMetadataOperationDirectly(RecordLayerConfig.getDefault(), applyState);
                }
            }
        };
    }

    private static Command getSetSchemaStateCommand(int lineNumber, @Nonnull final YamlExecutionContext executionContext, @Nonnull String value) {
        return new Command(lineNumber, executionContext) {
            @Override
            public void executeInternal(@Nonnull RelationalConnection connection) throws SQLException, RelationalException {
                logger.debug("⏳ Setting schema state '{}'", value);
                SchemaInstanceOuterClass.SchemaInstance schemaInstance = CommandUtil.fromJson(value);
                RecordLayerConfig rlConfig = new RecordLayerConfig.RecordLayerConfigBuilder()
                        .setIndexStateMap(CommandUtil.fromIndexStateProto(schemaInstance.getIndexStatesMap()))
                        .setFormatVersion(schemaInstance.getStoreInfo().getFormatVersion())
                        .setUserVersionChecker((oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(schemaInstance.getStoreInfo().getUserVersion()))
                        .build();
                ApplyState applyState = (RecordLayerMetadataOperationsFactory factory, Transaction txn) -> {
                    factory.getSetStoreStateConstantAction(URI.create(schemaInstance.getDatabaseId()), schemaInstance.getName()).execute(txn);
                };
                if (connection instanceof EmbeddedRelationalConnection) {
                    Command.applyMetadataOperationEmbedded((EmbeddedRelationalConnection) connection, rlConfig, applyState);
                } else {
                    Command.applyMetadataOperationDirectly(rlConfig, applyState);
                }
            }
        };
    }

    @FunctionalInterface
    private interface ApplyState {
        void applyMetadataOperation(RecordLayerMetadataOperationsFactory metadataOperationsFactory, Transaction transaction) throws RelationalException;
    }
}
