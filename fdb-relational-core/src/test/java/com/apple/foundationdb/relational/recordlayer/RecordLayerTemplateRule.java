/*
 * RecordLayerTemplateRule.java
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
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.function.Consumer;

public class RecordLayerTemplateRule implements BeforeEachCallback {
    private final RecordLayerCatalogRule catalogRule;
    private final String templateName;
    private final RecordMetaDataBuilder metaDataBuilder;

    public RecordLayerTemplateRule(String templateName, RecordLayerCatalogRule catalogRule) {
        this.templateName = templateName;
        this.catalogRule = catalogRule;
        this.metaDataBuilder = RecordMetaData.newBuilder();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        catalogRule.createSchemaTemplate(new RecordLayerTemplate(templateName, metaDataBuilder));
    }

    public RecordLayerTemplateRule setRecordFile(Descriptors.FileDescriptor descriptor) {
        metaDataBuilder.setRecords(descriptor);
        return this;
    }

    public RecordLayerTemplateRule configureTable(String tableName, Consumer<RecordTypeBuilder> builderConfig) {
        builderConfig.accept(metaDataBuilder.getRecordType(tableName));
        return this;
    }
}
