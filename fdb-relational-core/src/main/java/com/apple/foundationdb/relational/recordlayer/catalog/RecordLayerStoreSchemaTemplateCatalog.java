/*
 * RecordLayerStoreSchemaTemplateCatalog.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.RecordAlreadyExistsException;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaTemplateSystemTable;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Objects;

/**
 * RecordStore backed {@link SchemaTemplateCatalog}.
 * Pass in context to work against, the CATALOG schema and schemaPath.
 */
class RecordLayerStoreSchemaTemplateCatalog implements SchemaTemplateCatalog {

    @Nonnull
    private static final com.google.protobuf.ExtensionRegistry registry = com.google.protobuf.ExtensionRegistry.newInstance();

    static {
        registry.add(com.apple.foundationdb.record.RecordMetaDataOptionsProto.field);
        registry.add(com.apple.foundationdb.record.RecordMetaDataOptionsProto.record);
    }

    @Nonnull
    private final RecordLayerSchema catalogSchema;

    @Nonnull
    private final RelationalKeyspaceProvider.RelationalSchemaPath catalogSchemaPath;

    @Nonnull
    private final RecordMetaDataProvider catalogRecordMetaDataProvider;

    /**
     * Creates a RecordLayer-backed SchemaTemplateCatalog instance.
     *
     * @param catalogSchema the catalog context used as the operating environment
     * @param catalogSchemaPath the file system path to the schema context for operations
     * @throws RelationalException if schema template unwrapping fails due to type mismatch
     *                             (this should not occur under normal conditions)
     */
    @SpotBugsSuppressWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "Hard to remove exception with current inheritance")
    RecordLayerStoreSchemaTemplateCatalog(@Nonnull final RecordLayerSchema catalogSchema,
                                          @Nonnull final RelationalKeyspaceProvider.RelationalSchemaPath catalogSchemaPath) throws RelationalException {
        this.catalogSchema = catalogSchema;
        this.catalogSchemaPath = catalogSchemaPath;
        this.catalogRecordMetaDataProvider = RecordMetaData.build(this.catalogSchema.getSchemaTemplate()
                .unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata().toProto());
    }

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String schemaTemplateName)
            throws RelationalException {
        Tuple key = getSchemaTemplatePrimaryKey(schemaTemplateName);
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        try {
            try (RecordCursor<FDBStoredRecord<Message>> cursor =
                    recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE,
                                    EndpointType.RANGE_INCLUSIVE),
                            ContinuationImpl.BEGIN.getExecutionState(), ScanProperties.REVERSE_SCAN)) {
                RecordCursorResult<FDBStoredRecord<Message>> cursorResult = cursor.getNext();
                return cursorResult != null && !cursorResult.getContinuation().isEnd() && cursorResult.get() != null;
            }
        } catch (RecordCoreStorageException e) {
            throw new UncheckedRelationalException(ExceptionUtil.toRelationalException(e));
        }
    }

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String schemaTemplateName, int version)
            throws RelationalException {
        if (schemaTemplateName.equals(this.catalogSchema.getSchemaTemplate().getName()) &&
                version == this.catalogSchema.getSchemaTemplate().getVersion()) {
            // Catalog SchemaTemplate is Hard-coded.
            return true;
        }
        try {
            var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                    this.catalogRecordMetaDataProvider);
            return recordStore.loadRecord(getSchemaTemplatePrimaryKey(schemaTemplateName, version)) != null;
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    /**
     * Primary key for looking up {@link SchemaTemplate}s.
     * @param schemaTemplateName Name of the schema template.
     * @param version Version as int.
     * @return Tuple to use as primary key on the
     *  {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaTemplateSystemTable}
     */
    // TODO: Tie this to KeyExpression returned by SchemaTemplateSystemTable#getPrimaryKeyDefinition() rather than
    //  hand-make them as per here.
    // TODO: Is int for Version the right data type? Should it be long?
    private static Tuple getSchemaTemplatePrimaryKey(String schemaTemplateName, int version) {
        return Tuple.from(SystemTableRegistry.SCHEMA_TEMPLATE_RECORD_TYPE_KEY, schemaTemplateName, version);
    }

    /**
     * Primary key for looking up {@link SchemaTemplate}s.
     * @param schemaTemplateName Name of the schema template.
     * @return Tuple to use as primary key on the
     *  {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaTemplateSystemTable}
     */
    // TODO: Tie this to KeyExpression returned by SchemaTemplateSystemTable#getPrimaryKeyDefinition() rather than
    //  hand-make them as per here.
    private static Tuple getSchemaTemplatePrimaryKey(String schemaTemplateName) {
        return Tuple.from(SystemTableRegistry.SCHEMA_TEMPLATE_RECORD_TYPE_KEY, schemaTemplateName);
    }

    @Nonnull
    @Override
    public SchemaTemplate loadSchemaTemplate(@Nonnull final Transaction txn, @Nonnull final String templateName)
            throws RelationalException {
        final var key = getSchemaTemplatePrimaryKey(templateName);
        final var recordStore = RecordLayerStoreUtils.openRecordStore(txn, catalogSchemaPath, catalogRecordMetaDataProvider);
        final var tupleRange = new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
        try (var cursor = recordStore.scanRecords(tupleRange, ContinuationImpl.BEGIN.getExecutionState(), ScanProperties.REVERSE_SCAN)) {
            final var cursorResult = cursor.getNext();
            final var schemaExists = !cursorResult.getContinuation().isEnd() && cursorResult.get() != null;
            Assert.thatUnchecked(schemaExists, ErrorCode.UNKNOWN_SCHEMA_TEMPLATE,
                    "SchemaTemplate '" + templateName + "' is not in catalog");
            return toSchemaTemplate(Assert.notNullUnchecked(cursorResult.get()).getRecord());
        } catch (RecordCoreStorageException | InvalidProtocolBufferException e) {
            throw new UncheckedRelationalException(ExceptionUtil.toRelationalException(e));
        }
    }

    @Nonnull
    @Override
    public SchemaTemplate loadSchemaTemplate(@Nonnull final Transaction txn, @Nonnull final String templateName, int version)
            throws RelationalException {
        try {
            // TODO: I seem to be doing way more work than I should have to. Someone please set me right. Stack 05/2023.
            var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                    this.catalogRecordMetaDataProvider);
            var fdbSR = recordStore.loadRecord(getSchemaTemplatePrimaryKey(templateName, version));
            if (fdbSR == null) {
                throw new RelationalException("SchemaTemplate=" + templateName + ", version=" + version +
                        " is not in catalog", ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
            }
            return toSchemaTemplate(fdbSR.getRecord());
        } catch (InvalidProtocolBufferException | RecordCoreException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    /**
     * Instantiate an instance of {@link SchemaTemplate} using content of the passed {@link Message}.
     */
    @Nonnull
    private static SchemaTemplate toSchemaTemplate(@Nonnull final Message message) throws InvalidProtocolBufferException {
        // we should probably memoize a Message -> RecordLayerSchemaTemplate relation to avoid repetitive
        // deserialization of the same message over and over again.
        final Descriptors.Descriptor descriptor = message.getDescriptorForType();
        final ByteString bs = Assert.castUnchecked(message.getField(descriptor.findFieldByName(SchemaTemplateSystemTable.METADATA)), ByteString.class);
        final RecordMetaData metaData = RecordMetaData.newBuilder().setRecords(RecordMetaDataProto.MetaData.parseFrom(bs.toByteArray(), registry)).getRecordMetaData();
        final String name = message.getField(descriptor.findFieldByName(SchemaTemplateSystemTable.TEMPLATE_NAME)).toString();
        int templateVersion = (int) message.getField(descriptor.findFieldByName(SchemaTemplateSystemTable.TEMPLATE_VERSION));
        return RecordLayerSchemaTemplate.fromRecordMetadata(metaData, name, templateVersion);
    }

    @Override
    @SuppressWarnings("deprecation") // need to replace protobuf data builder
    public void createTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate newTemplate) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        Assert.notNull(recordStore);
        try {
            ProtobufDataBuilder pmd = new ProtobufDataBuilder(this.catalogRecordMetaDataProvider.getRecordMetaData()
                    .getRecordType(SystemTableRegistry.SCHEMA_TEMPLATE_TABLE_NAME).getDescriptor());
            pmd.setField(SchemaTemplateSystemTable.TEMPLATE_NAME, newTemplate.getName());
            pmd.setField(SchemaTemplateSystemTable.TEMPLATE_VERSION, newTemplate.getVersion());
            // Is this how serialization of the metadata is supposed to be done?
            RecordMetaData metaData = newTemplate.unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata();
            pmd.setField(SchemaTemplateSystemTable.METADATA, metaData.toProto().toByteString());
            recordStore.saveRecord(pmd.build(), FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS);
        } catch (RecordAlreadyExistsException e) {
            throw new RelationalException("Schema template already exists: " + newTemplate.getName(), ErrorCode.DUPLICATE_SCHEMA_TEMPLATE, e);
        } catch (RecordCoreException | SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    @SuppressWarnings("PMD.CloseResource") // lifetime of cursor extends into lifetime of returned result set
    @Override
    public RelationalResultSet listTemplates(@Nonnull Transaction txn) {
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_TEMPLATE_RECORD_TYPE_KEY);
        try {
            var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                    this.catalogRecordMetaDataProvider);
            RecordCursor<FDBStoredRecord<Message>> cursor =
                    recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE,
                                    EndpointType.RANGE_INCLUSIVE),
                            ContinuationImpl.BEGIN.getExecutionState(), ScanProperties.FORWARD_SCAN);
            Descriptors.Descriptor d = recordStore.getRecordMetaData().getRecordMetaData()
                    .getRecordType(SchemaTemplateSystemTable.TABLE_NAME).getDescriptor();
            final var structMetaData = RelationalStructMetaData.of((DataType.StructType) DataTypeUtils.toRelationalType(ProtobufDdlUtil.recordFromDescriptor(d)));
            return new RecordLayerResultSet(structMetaData,
                    RecordLayerIterator.create(cursor, this::transformSchemaTemplates),
                    null /* caller is responsible for managing tx state */);
        } catch (RecordCoreStorageException | RelationalException e) {
            throw new UncheckedRelationalException(ExceptionUtil.toRelationalException(e));
        }
    }

    private Row transformSchemaTemplates(@Nullable FDBStoredRecord<Message> record) {
        if (record == null) {
            return null;
        }
        Message m = record.getRecord();
        final RecordMetaData recordMetaData = this.catalogRecordMetaDataProvider.getRecordMetaData();
        final RecordType recordType = recordMetaData.getRecordType(SchemaTemplateSystemTable.TABLE_NAME);
        final Descriptors.Descriptor descriptor = recordType.getDescriptor();
        String name = (String) m.getField(descriptor.findFieldByName(SchemaTemplateSystemTable.TEMPLATE_NAME));
        final var version = (Integer) m.getField(descriptor.findFieldByName(SchemaTemplateSystemTable.TEMPLATE_VERSION));
        ByteString metaData = (ByteString) m.getField(descriptor.findFieldByName(SchemaTemplateSystemTable.METADATA));
        return new ArrayRow(name, version, metaData.toByteArray());
    }

    @Override
    public void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateName, boolean throwIfDoesNotExist) throws RelationalException {
        Tuple key = getSchemaTemplatePrimaryKey(templateName);
        try {
            var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                    this.catalogRecordMetaDataProvider);
            try (RecordCursor<FDBStoredRecord<Message>> cursor =
                    recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE,
                                    EndpointType.RANGE_INCLUSIVE),
                            ContinuationImpl.BEGIN.getExecutionState(), ScanProperties.FORWARD_SCAN);) {
                RecordCursorResult<FDBStoredRecord<Message>> cursorResult;
                boolean deletedSomething = false;
                do {
                    cursorResult = cursor.getNext();
                    if (cursorResult.getContinuation().isEnd()) {
                        break;
                    }
                    Tuple primaryKey = Objects.requireNonNull(cursorResult.get()).getPrimaryKey();
                    if (!recordStore.deleteRecord(primaryKey)) {
                        throw new RelationalException("Schema template record should exist but didn't when trying to delete it", ErrorCode.INTERNAL_ERROR);
                    }
                    deletedSomething = true;
                } while (cursorResult.hasNext());
                if (!deletedSomething && throwIfDoesNotExist) {
                    throw new RelationalException("Could not delete unknown schema template " + templateName, ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
                }
            }
        } catch (RecordCoreStorageException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    @Override
    public void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateName, int version, boolean throwIfDoesNotExist)
            throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        if (!recordStore.deleteRecord(getSchemaTemplatePrimaryKey(templateName, version)) && throwIfDoesNotExist) {
            throw new RelationalException("Could not delete unknown schema template " + templateName, ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
    }
}
