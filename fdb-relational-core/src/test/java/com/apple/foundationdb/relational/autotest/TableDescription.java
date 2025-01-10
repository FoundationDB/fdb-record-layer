/*
 * TableDescription.java
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

package com.apple.foundationdb.relational.autotest;

import com.apple.foundationdb.relational.api.StructMetaData;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;

public class TableDescription {
    private final StructMetaData metaData;
    private final List<String> pkColumns;

    public TableDescription(@Nonnull StructMetaData metaData, @Nonnull List<String> pkColumns) {
        this.metaData = metaData;
        this.pkColumns = pkColumns;
    }

    public String getTableName() throws SQLException {
        return metaData.getTypeName();
    }

    public StructMetaData getMetaData() {
        return metaData;
    }

    public List<String> getPkColumns() {
        return pkColumns;
    }
}
