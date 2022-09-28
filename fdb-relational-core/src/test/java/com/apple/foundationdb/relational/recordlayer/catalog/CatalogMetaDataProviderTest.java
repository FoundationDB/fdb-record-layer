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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.DirectFdbConnection;
import com.apple.foundationdb.relational.recordlayer.FdbConnection;
import com.apple.foundationdb.relational.recordlayer.KeySpaceExtension;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;
import com.apple.foundationdb.relational.utils.DescriptorAssert;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.List;
import java.util.Optional;

/**
 * Tests around the StoreCatalogMetaData provider logic. The intent here is to test that
 * we can load a RecordMetaData object based off of StoreCatalog information.
 */
class CatalogMetaDataProviderTest {
    @RegisterExtension
    public static final KeySpaceExtension keySpaceExt = new KeySpaceExtension();

    @Test
    void canLoadMetaDataFromStore() throws RelationalException, Descriptors.DescriptorValidationException {

        RecordLayerStoreCatalogImpl catalog = new RecordLayerStoreCatalogImpl(keySpaceExt.getKeySpace());

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

        SchemaTemplate schemaTemplate = createSchemaTemplate();
        try (Transaction txn = fdbConn.getTransactionManager().createTransaction(Options.NONE)) {
            //write schema info to the store
            Schema schema = schemaTemplate.generateSchema(dbUri.getPath(), schemaName);
            catalog.createDatabase(txn, dbUri);
            catalog.updateSchema(txn, schema);

            CatalogMetaDataProvider metaDataProvider = new CatalogMetaDataProvider(catalog, dbUri, schemaName, txn);
            final RecordMetaData recordMetaData = metaDataProvider.getRecordMetaData();
            final Descriptors.FileDescriptor descriptor = recordMetaData.getRecordsDescriptor();

            Descriptors.FileDescriptor expected = Descriptors.FileDescriptor.buildFrom(
                    schemaTemplate.toProtobufDescriptor(),
                    new Descriptors.FileDescriptor[]{RecordMetaDataProto.getDescriptor()});

            for (Descriptors.Descriptor message : descriptor.getMessageTypes()) {
                new DescriptorAssert(message).as("Incorrect descriptor for type %s", message.getName())
                        .isContainedIn(expected.getMessageTypes());
            }
        }
    }

    private SchemaTemplate createSchemaTemplate() throws RelationalException {
        TypingContext.FieldDefinition fieldDefinition = new TypingContext.FieldDefinition("col1", Type.TypeCode.STRING, "RESTAURANT", false);

        TypingContext.TypeDefinition typeDefinition = new TypingContext.TypeDefinition("RESTAURANT", List.of(fieldDefinition), true, Optional.of(List.of("col1")));

        TypingContext typingContext = TypingContext.create();
        typingContext.addType(typeDefinition);
        typingContext.addAllToTypeRepository();

        return typingContext.generateSchemaTemplate("testTemplate", 1L);
    }
}
