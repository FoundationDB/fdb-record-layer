/*
 * CatalogMetaDataProviderTest.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.recordlayer.DirectFdbConnection;
import com.apple.foundationdb.relational.recordlayer.FdbConnection;
import com.apple.foundationdb.relational.recordlayer.KeySpaceExtension;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.utils.DescriptorAssert;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

/**
 * Tests around the StoreCatalogMetaData provider logic. The intent here is to test that
 * we can load a RecordMetaData object based off of StoreCatalog information.
 */
class CatalogMetaDataProviderTest {
    @RegisterExtension
    public static final KeySpaceExtension keySpaceExt = new KeySpaceExtension();

    @Test
    void canLoadMetaDataFromStore() throws RelationalException, Descriptors.DescriptorValidationException {
        SchemaTemplateCatalog templateCatalog = new InMemorySchemaTemplateCatalog();
        RecordLayerStoreCatalogImpl catalog = new RecordLayerStoreCatalogImpl(keySpaceExt.getKeySpace(), templateCatalog);

        //now create a RecordStore in that Catalog
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        FdbConnection fdbConn = new DirectFdbConnection(factory.getDatabase());
        try (Transaction txn = fdbConn.getTransactionManager().createTransaction(Options.NONE)) {
            //create the Catalog RecordStore
            catalog.initialize(txn);
            txn.commit();
        }

        URI dbUri = URI.create("/testdb");
        String schemaName = "TEST_SCHEMA" + System.currentTimeMillis();

        RecordLayerSchemaTemplate schemaTemplate = createSchemaTemplate();
        try (Transaction txn = fdbConn.getTransactionManager().createTransaction(Options.NONE)) {
            //write template into template catalog
            templateCatalog.updateTemplate(txn, schemaTemplate);
            //write schema info to the store
            Schema schema = schemaTemplate.generateSchema(dbUri.getPath(), schemaName);
            catalog.createDatabase(txn, dbUri);
            catalog.saveSchema(txn, schema);

            CatalogMetaDataProvider metaDataProvider = new CatalogMetaDataProvider(catalog, dbUri, schemaName, txn);
            final RecordMetaData recordMetaData = metaDataProvider.getRecordMetaData();
            final Descriptors.FileDescriptor descriptor = recordMetaData.getRecordsDescriptor();

            Descriptors.FileDescriptor expected = Descriptors.FileDescriptor.buildFrom(
                    schemaTemplate.toRecordMetadata().getRecordsDescriptor().toProto(), // not sure this is correct
                    new Descriptors.FileDescriptor[]{RecordMetaDataProto.getDescriptor()});

            for (Descriptors.Descriptor message : descriptor.getMessageTypes()) {
                new DescriptorAssert(message).as("Incorrect descriptor for type %s", message.getName())
                        .isContainedIn(expected.getMessageTypes());
            }
        }
    }

    private RecordLayerSchemaTemplate createSchemaTemplate() throws RelationalException {
        return RecordLayerSchemaTemplate
                .newBuilder()
                .addTable(
                        RecordLayerTable
                                .newBuilder()
                                .addColumn(
                                        RecordLayerColumn
                                                .newBuilder()
                                                .setName("col1")
                                                .setDataType(DataType.Primitives.STRING.type())
                                                .build())
                                .setName("RESTAURANT")
                                .build())
                .setVersion(1L)
                .setName("testTemplate")
                .build();
    }
}
