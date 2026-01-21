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
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
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
import com.apple.foundationdb.relational.yamltests.YamsqlReference;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.generated.schemainstance.SchemaInstanceOuterClass;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings({"PMD.AvoidCatchingThrowable"})
public abstract class Command {

    public static final String COMMAND_LOAD_SCHEMA_TEMPLATE = "load schema template";
    public static final String COMMAND_SET_SCHEMA_STATE = "set schema state";
    public static final String COMMAND_QUERY = "query";

    private static final Logger logger = LogManager.getLogger(Command.class);

    @Nonnull
    private final YamsqlReference reference;
    @Nonnull
    final YamlExecutionContext executionContext;

    Command(@Nonnull final YamsqlReference reference, @Nonnull YamlExecutionContext executionContext) {
        this.reference = reference;
        this.executionContext = executionContext;
    }

    @Nonnull
    public YamsqlReference getReference() {
        return reference;
    }

    @Nonnull
    public static Command parse(@Nonnull final YamsqlReference.YamsqlResource resource, @Nonnull Object object, @Nonnull final String blockName, @Nonnull final YamlExecutionContext executionContext) {
        final var command = Matchers.notNull(Matchers.firstEntry(Matchers.arrayList(object, "command").get(0), "command"), "command");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(command.getKey(), () -> "Invalid command key-value pair: " + command);
        final var reference = resource.withLineNumber(linedObject.getLineNumber());
        try {
            final var key = Matchers.notNull(Matchers.string(linedObject.getObject(), "command"), "command");
            final var value = Matchers.notNull(command, "query configuration").getValue();
            switch (key) {
                case COMMAND_LOAD_SCHEMA_TEMPLATE:
                    return getLoadSchemaTemplateCommand(reference, executionContext, (String) value);
                case COMMAND_SET_SCHEMA_STATE:
                    return getSetSchemaStateCommand(reference, executionContext, (String) value);
                case COMMAND_QUERY:
                    return QueryCommand.parse(resource, object, blockName, executionContext);
                default:
                    Assert.failUnchecked(String.format(Locale.ROOT, "Could not find command '%s'", key));
                    return null;
            }
        } catch (Exception e) {
            throw executionContext.wrapContext(e, () -> "‼️ Error parsing command at " + reference, "command", reference);
        }
    }

    public final void execute(@Nonnull final YamlConnection connection) {
        try {
            executeInternal(connection);
        } catch (Throwable e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️ Error executing command at " + getReference() + " against connection for versions " + connection.getVersions(),
                    "command", getReference());
        }
    }

    abstract void executeInternal(@Nonnull YamlConnection connection) throws SQLException, RelationalException;

    private static void applyMetadataOperationEmbedded(@Nonnull EmbeddedRelationalConnection connection,
                                                       @Nonnull RecordLayerConfig rlConfig, @Nonnull ApplyState applyState)
            throws SQLException, RelationalException {
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

    private static void applyMetadataOperationDirectly(@Nonnull RecordLayerConfig rlConfig, @Nonnull ApplyState applyState,
                                                       @Nullable String clusterFile) throws RelationalException {
        final FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase(clusterFile);
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

    @SuppressWarnings({"PMD.CloseResource"}) // We "borrow" from the connection via tryGetEmbedded, but the connection will close it
    private static Command getLoadSchemaTemplateCommand(@Nonnull final YamsqlReference reference, @Nonnull final YamlExecutionContext executionContext, @Nonnull String value) {
        return new Command(reference, executionContext) {
            @Override
            public void executeInternal(@Nonnull YamlConnection connection) throws SQLException, RelationalException {
                logger.debug("⏳ Loading template '{}'", value);
                // current connection should be __SYS/catalog
                // save schema template
                ApplyState applyState = (RecordLayerMetadataOperationsFactory factory, Transaction txn) -> {
                    final var options = Options.none();
                    final var schemaTemplate = CommandUtil.fromProto(value);
                    factory.getSaveSchemaTemplateConstantAction(schemaTemplate, options).execute(txn);
                };
                final EmbeddedRelationalConnection embedded = connection.tryGetEmbedded();
                if (embedded != null) {
                    Command.applyMetadataOperationEmbedded(embedded, RecordLayerConfig.getDefault(), applyState);
                } else {
                    Command.applyMetadataOperationDirectly(RecordLayerConfig.getDefault(), applyState, connection.getClusterFile());
                }
            }
        };
    }

    @SuppressWarnings({"PMD.CloseResource"}) // We "borrow" from the connection via tryGetEmbedded, but the connection will close it
    private static Command getSetSchemaStateCommand(@Nonnull final YamsqlReference reference, @Nonnull final YamlExecutionContext executionContext, @Nonnull String value) {
        return new Command(reference, executionContext) {
            @Override
            public void executeInternal(@Nonnull YamlConnection connection) throws SQLException, RelationalException {
                logger.debug("⏳ Setting schema state '{}'", value);
                SchemaInstanceOuterClass.SchemaInstance schemaInstance = CommandUtil.fromJson(value);
                RecordLayerConfig rlConfig = new RecordLayerConfig.RecordLayerConfigBuilder()
                        .setIndexStateMap(CommandUtil.fromIndexStateProto(schemaInstance.getIndexStatesMap()))
                        .setFormatVersion(FormatVersion.getFormatVersion(schemaInstance.getStoreInfo().getFormatVersion()))
                        .setUserVersionChecker((oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(schemaInstance.getStoreInfo().getUserVersion()))
                        .build();
                ApplyState applyState = (RecordLayerMetadataOperationsFactory factory, Transaction txn) -> {
                    factory.getSetStoreStateConstantAction(URI.create(schemaInstance.getDatabaseId()), schemaInstance.getName()).execute(txn);
                };
                final EmbeddedRelationalConnection embedded = connection.tryGetEmbedded();
                if (embedded != null) {
                    Command.applyMetadataOperationEmbedded(embedded, rlConfig, applyState);
                } else {
                    Command.applyMetadataOperationDirectly(rlConfig, applyState, connection.getClusterFile());
                }
            }
        };
    }

    @FunctionalInterface
    private interface ApplyState {
        void applyMetadataOperation(RecordLayerMetadataOperationsFactory metadataOperationsFactory, Transaction transaction) throws RelationalException;
    }
}
