/*
 * RecordTypeMetaData.java
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

import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.relational.api.catalog.IndexMetaData;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.recordlayer.ddl.ColumnDescriptor;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public class RecordTypeMetaData implements TableMetaData {
    private final RecordType recordType;

    public RecordTypeMetaData(RecordType recordType) {
        this.recordType = recordType;
    }

    @Nonnull
    @Override
    public ColumnDescriptor[] getColumns() {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Nonnull
    @Override
    public ColumnDescriptor[] getKeyColumns() {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Nonnull
    @Override
    public Set<IndexMetaData> getIndexes() {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Nullable
    @Override
    public Descriptors.Descriptor getTableTypeDescriptor() {
        return recordType.getDescriptor();
    }
}
