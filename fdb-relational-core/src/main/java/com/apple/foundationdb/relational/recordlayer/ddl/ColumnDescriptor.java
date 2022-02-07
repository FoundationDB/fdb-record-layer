/*
 * ColumnDescriptor.java
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

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Descriptor for a Column type.
 */
public class ColumnDescriptor {
    final String name;
    final DataType dataType;
    final boolean isRepeated;
    final boolean isPrimaryKey;

    public ColumnDescriptor(@Nonnull String name, @Nonnull DataType dataType, boolean isRepeated, boolean isPrimaryKey) {
        this.name = name;
        this.dataType = dataType;
        this.isRepeated = isRepeated;
        this.isPrimaryKey = isPrimaryKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnDescriptor that = (ColumnDescriptor) o;
        return isRepeated == that.isRepeated && isPrimaryKey == that.isPrimaryKey && name.equals(that.name) && dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, isRepeated, isPrimaryKey);
    }
}
