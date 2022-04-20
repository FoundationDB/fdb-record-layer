/*
 * TableInfo.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.generated.CatalogData;

import com.google.protobuf.DescriptorProtos;

import java.util.stream.Collectors;

public class TableInfo {
    String tableName;
    CatalogData.Table table;
    DescriptorProtos.DescriptorProto descriptor;

    public TableInfo(String tableName,
                     CatalogData.Table table,
                     DescriptorProtos.DescriptorProto descriptor) {
        this.tableName = tableName;
        this.table = table;
        this.descriptor = descriptor;
    }

    public String getTableName() {
        return tableName;
    }

    public CatalogData.Table getTable() {
        return table;
    }

    public DescriptorProtos.DescriptorProto toDescriptor() {
        return descriptor;
    }

    @Override
    public String toString() {
        String toStr =  "table: {name: " + descriptor.getName() + ", columns: { ";
        toStr += descriptor.getFieldList().stream().map(field -> field.getName() + ":" + ProtobufDdlUtil.getTypeName(field)).collect(Collectors.joining(","));

        toStr += "}}";
        return toStr;
    }
}
