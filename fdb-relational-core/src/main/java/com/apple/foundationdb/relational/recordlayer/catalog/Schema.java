/*
 * Schema.java
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

import com.apple.foundationdb.record.RecordMetaDataProto;

import java.util.Set;
import java.util.stream.Collectors;

public class Schema {
    private final RecordMetaDataProto.MetaData metaData;
    private final String linkedTemplate;
    private final long templateVersion; //a snapshot of the version of the template which this schema was _last_ updated to

    private final String databaseId;
    private final String schemaName;

    public Schema(String databaseId,
                  String schemaName,
                  RecordMetaDataProto.MetaData metaData,
                  String linkedTemplate,
                  long templateVersion) {
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.metaData = metaData;
        this.linkedTemplate = linkedTemplate;
        this.templateVersion = templateVersion;
    }

    public RecordMetaDataProto.MetaData getMetaData() {
        return metaData;
    }

    public String getSchemaTemplateName() {
        return linkedTemplate;
    }

    public long getTemplateVersion() {
        return templateVersion;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public Set<String> getTableNames() {
        return metaData.getRecordTypesList().stream()
                .map(RecordMetaDataProto.RecordType::getName)
                .collect(Collectors.toSet());
    }
}
