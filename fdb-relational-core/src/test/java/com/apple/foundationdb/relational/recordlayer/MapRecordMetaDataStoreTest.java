/*
 * MapRecordMetaDataStoreTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public class MapRecordMetaDataStoreTest {
    @Test
    void testMapRecordMetaDataStore() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        MutableRecordMetaDataStore metaDataStore = new MapRecordMetaDataStore();
        RecordLayerTemplate template = new RecordLayerTemplate("testTemplate", builder.build());

        metaDataStore.addSchemaTemplate(template);
        metaDataStore.assignSchemaToTemplate(URI.create("/db/testSchema"), "testTemplate");

        List<String> recordTypes = metaDataStore.loadMetaData(URI.create("/db/testSchema"))
                .getRecordMetaData().getRecordTypes().values().stream().map(r -> r.getName()).collect(Collectors.toList());
        Assertions.assertLinesMatch(ImmutableList.of("RestaurantReviewer", "RestaurantRecord"), recordTypes);
    }
}
