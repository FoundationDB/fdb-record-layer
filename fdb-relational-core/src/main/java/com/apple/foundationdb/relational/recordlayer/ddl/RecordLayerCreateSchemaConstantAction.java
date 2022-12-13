/*
 * RecordLayerCreateSchemaConstantAction.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreAlreadyExistsException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.catalog.CatalogMetaDataProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalog;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import java.net.URI;

/**
 * Will eventually remove CreateSchemaConstantAction and replace it with this.
 */
public class RecordLayerCreateSchemaConstantAction implements ConstantAction {
    private final StoreCatalog catalog;
    private final SchemaTemplateCatalog templateCatalog;
    private final RecordLayerConfig rlConfig;
    private final URI dbUri;
    private final String schemaName;
    private final String templateName;
    private final KeySpace keySpace;

    public RecordLayerCreateSchemaConstantAction(URI dbUri,
                                                 String schemaName,
                                                 String templateName,
                                                 RecordLayerConfig rlConfig,
                                                 KeySpace keySpace,
                                                 StoreCatalog catalog,
                                                 SchemaTemplateCatalog templateCatalog) {
        this.schemaName = schemaName;
        this.templateName = templateName;
        this.catalog = catalog;
        this.templateCatalog = templateCatalog;
        this.dbUri = dbUri;
        this.rlConfig = rlConfig;
        this.keySpace = keySpace;
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //can't without violating Record layer isolation law
    public void execute(Transaction txn) throws RelationalException {
        /*
         *
         */
        if (!catalog.doesDatabaseExist(txn, dbUri)) {
            throw new RelationalException(String.format("Database %s does not exist", dbUri.getPath()), ErrorCode.UNDEFINED_DATABASE);
        }
        //verify that the schema doesn't already exist
        // This is a bit awkward--perhaps we should adjust the behavior of the StoreCatalog?
        try {
            final Schema beforeSchema = catalog.loadSchema(txn, dbUri, schemaName);
            String schemaTemplateName = beforeSchema.getSchemaTemplate().getName();
            throw new RelationalException("Schema " + schemaName + " already exists with mapping " + schemaTemplateName, ErrorCode.SCHEMA_ALREADY_EXISTS);
        } catch (RelationalException ve) {
            if (ve.getErrorCode() != ErrorCode.UNDEFINED_SCHEMA) {
                throw ve;
            }
        }

        final SchemaTemplate schemaTemplate = templateCatalog.loadTemplate(txn, templateName);

        //map the schema to the template
        final Schema schema = schemaTemplate.generateSchema(dbUri.getPath(), schemaName);

        //insert the schema into the catalog
        catalog.saveSchema(txn, schema);

        //now create the FDBRecordStore
        KeySpacePath ksPath = KeySpaceUtils.uriToPath(dbUri, keySpace).add("schema", schemaName);
        try {
            FDBRecordStore.newBuilder()
                    .setKeySpacePath(ksPath)
                    .setSerializer(rlConfig.getSerializerRegistry().loadSerializer(ksPath))
                    .setMetaDataProvider(new CatalogMetaDataProvider(catalog, dbUri, schemaName, txn))
                    .setUserVersionChecker(rlConfig.getUserVersionChecker())
                    .setFormatVersion(rlConfig.getFormatVersion())
                    .setContext(txn.unwrap(FDBRecordContext.class))
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS);
        } catch (RecordStoreAlreadyExistsException rsaee) {
            // The schema already exists!
            throw new RelationalException("Schema <" + schemaName + "> already exists", ErrorCode.SCHEMA_ALREADY_EXISTS);
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

}
