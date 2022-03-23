/*
 * RecordLayerCatalogBuilderTest.java
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


import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.SerializerRegistry;
import com.apple.foundationdb.relational.recordlayer.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RecordLayerCatalogBuilderTest {
    RecordLayerCatalog.Builder builder = new RecordLayerCatalog.Builder()
            .setMetadataProvider(Mockito.mock(MutableRecordMetaDataStore.class))
            .setUserVersionChecker(Mockito.mock(FDBRecordStoreBase.UserVersionChecker.class))
            .setSerializerRegistry(Mockito.mock(SerializerRegistry.class))
            .setDatabaseLocator(Mockito.mock(DatabaseLocator.class))
            .setKeySpace(Mockito.mock(KeySpace.class))
            .setFormatVersion(0)
            .setStoreTimer(Mockito.mock(FDBStoreTimer.class));

    @Test
    void allArguments() {
        Assertions.assertDoesNotThrow(() -> builder.build());
    }

    @Test
    void missingMetadataProvider() {
        builder.setMetadataProvider(null);
        RelationalAssertions.assertThrowsRelationalException(() -> builder.build(), ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void missingSerializerRegistry() {
        builder.setSerializerRegistry(null);
        RelationalAssertions.assertThrowsRelationalException(() -> builder.build(), ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void missingKeySpace() {
        builder.setKeySpace(null);
        RelationalAssertions.assertThrowsRelationalException(() -> builder.build(), ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void formatVersion() {
        builder.setFormatVersion(-1);
        Assertions.assertDoesNotThrow(() -> builder.build());
        builder.setFormatVersion(12);
        Assertions.assertDoesNotThrow(() -> builder.build());
    }

}
