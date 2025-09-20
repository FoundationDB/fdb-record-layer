/*
 * StoreConfig.java
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

package com.apple.foundationdb.relational.recordlayer.storage;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializer;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializerJCE;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.NoSuchDirectoryException;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.google.protobuf.Message;

import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.zip.Deflater;

@API(API.Status.EXPERIMENTAL)
public final class StoreConfig {
    public static final RecordSerializer<Message> DEFAULT_RELATIONAL_SERIALIZER = TransformedRecordSerializer.newDefaultBuilder()
            .setEncryptWhenSerializing(false)
            .setCompressWhenSerializing(true)
            .setCompressionLevel(Deflater.DEFAULT_COMPRESSION)
            .setWriteValidationRatio(0.0)
            .build();
    private final RecordLayerConfig recordLayerConfig;
    private final String schemaName;
    private final KeySpacePath storePath;
    private final RecordMetaDataProvider metaDataProvider;
    private final RecordSerializer<Message> serializer;

    private StoreConfig(RecordLayerConfig recordLayerConfig,
                        String schemaName,
                        KeySpacePath storePath,
                        RecordMetaDataProvider metaDataProvider,
                        RecordSerializer<Message> serializer) {
        this.recordLayerConfig = recordLayerConfig;
        this.schemaName = schemaName;
        this.storePath = storePath;
        this.metaDataProvider = metaDataProvider;
        this.serializer = serializer;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public KeySpacePath getStorePath() {
        return storePath;
    }

    public RecordMetaDataProvider getMetaDataProvider() {
        return metaDataProvider;
    }

    public RecordSerializer<Message> getSerializer() {
        return serializer;
    }

    public FDBRecordStoreBase.UserVersionChecker getUserVersionChecker() {
        return recordLayerConfig.getUserVersionChecker();
    }

    public FormatVersion getFormatVersion() {
        return recordLayerConfig.getFormatVersion();
    }

    public static StoreConfig create(RecordLayerConfig recordLayerConfig,
                                     String schemaName,
                                     RelationalKeyspaceProvider.RelationalDatabasePath databasePath,
                                     RecordMetaDataStore metaDataStore,
                                     Transaction transaction,
                                     Options options) throws RelationalException {
        //TODO(bfines) error handling if this store doesn't exist

        RelationalKeyspaceProvider.RelationalSchemaPath schemaPath;
        try {
            schemaPath = databasePath.schemaPath(schemaName);
        } catch (NoSuchDirectoryException nsde) {
            throw new RelationalException("Uninitialized Catalog", ErrorCode.INTERNAL_ERROR, nsde);
        } catch (MetaDataException mde) {
            throw new RelationalException(mde.getMessage(), ErrorCode.UNDEFINED_SCHEMA, mde);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }

        URI dbUri = databasePath.toUri();
        RecordMetaDataProvider metaDataProvider = metaDataStore.loadMetaData(transaction, dbUri, schemaName);

        RecordSerializer<Message> serializer = serializerFromOptions(options);

        return new StoreConfig(recordLayerConfig, schemaName, schemaPath, metaDataProvider, serializer);
    }

    static RecordSerializer<Message> serializerFromOptions(Options options) throws RelationalException {
        final boolean encrypted = options.getOption(Options.Name.ENCRYPT_WHEN_SERIALIZING);
        final boolean compressed = options.getOption(Options.Name.COMPRESS_WHEN_SERIALIZING);
        final String keyStoreFile = options.getOption(Options.Name.ENCRYPTION_KEY_STORE);
        if (!encrypted && compressed && keyStoreFile == null) {
            return DEFAULT_RELATIONAL_SERIALIZER;
        }
        final TransformedRecordSerializerJCE.Builder<Message> serializerBuilder = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(encrypted)
                .setCompressWhenSerializing(compressed)
                .setCompressionLevel(Deflater.DEFAULT_COMPRESSION)
                .setWriteValidationRatio(0.0);
        if (keyStoreFile != null) {
            final SecretKey key;
            try {
                final String keyEntryAlias = options.getOption(Options.Name.ENCRYPTION_KEY_ENTRY);
                if (keyEntryAlias == null) {
                    throw new RelationalException("Key entry not specified", ErrorCode.UNSUPPORTED_OPERATION);
                }
                final String keyPassword = options.getOption(Options.Name.ENCRYPTION_KEY_PASSWORD);
                KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
                    keystore.load(fis, keyPassword.toCharArray());
                }
                KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(keyPassword.toCharArray());
                KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry)keystore.getEntry(keyEntryAlias, protParam);
                key = entry.getSecretKey();
            } catch (FileNotFoundException ex) {
                throw new RelationalException("Key store not found", ErrorCode.UNSUPPORTED_OPERATION, ex);
            } catch (GeneralSecurityException | IOException ex) {
                throw new RelationalException("Key loading failed", ErrorCode.UNSUPPORTED_OPERATION, ex);
            }
            serializerBuilder.setEncryptionKey(key);
        } else if (encrypted) {
            throw new RelationalException("Key store not specified", ErrorCode.UNSUPPORTED_OPERATION);
        }
        return serializerBuilder.build();
    }
}
