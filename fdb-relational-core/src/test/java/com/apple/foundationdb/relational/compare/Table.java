/*
 * Table.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.compare;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

class Table {
    private final String tableName;
    private final List<Column> columns;
    @Nullable
    private final Descriptors.Descriptor descriptor;

    @Nullable
    private final Message defaultMessageType;

    public Table(String tableName, List<Column> columns) {
        this(tableName, columns, null, null);
    }

    public Table(String tableName, List<Column> columns, @Nullable Descriptors.Descriptor descriptor, @Nullable Message defaultMessageType) {
        this.tableName = tableName;
        this.columns = columns;
        this.descriptor = descriptor;
        this.defaultMessageType = defaultMessageType;
    }

    @Nullable
    public Descriptors.Descriptor getDescriptor() {
        return descriptor;
    }

    @Nullable
    public Message.Builder getProtobuf() {
        if (defaultMessageType == null) {
            return null;
        }
        return defaultMessageType.newBuilderForType();
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getCreateStatement() {
        StringBuilder statement = new StringBuilder("CREATE TABLE ").append(tableName).append("(");
        boolean isFirst = true;
        for (Column column : columns) {
            if (!isFirst) {
                statement.append(",");
            } else {
                isFirst = false;
            }
            statement.append(column.columnName).append(" ").append(column.columnType);
        }
        statement.append(")");

        return statement.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table that = (Table) o;
        return tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName);
    }

    public String getName() {
        return tableName;
    }

}
