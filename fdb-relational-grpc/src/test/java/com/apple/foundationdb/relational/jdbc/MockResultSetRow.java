/*
 * MockResultSetRow.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalStruct;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A testing implementation of a result set row.
 * This class assumes nothing about the content, which means that trying to get a wrong type from
 * a column will throw ClassCastException, and trying to get a column beyond the index will
 * throw IndexOutOfBoundsException
 */
public class MockResultSetRow {
    @Nonnull
    private final List<?> row;

    public MockResultSetRow(@Nonnull List<?> row) {
        this.row = row;
    }

    public boolean wasNull() {
        return false;
    }

    public String getString(int columnIndex) {
        return (String) row.get(columnIndex - 1);
    }

    public boolean getBoolean(int columnIndex) {
        return (boolean) row.get(columnIndex - 1);
    }

    public int getInt(int columnIndex) {
        return (int) row.get(columnIndex - 1);
    }

    public long getLong(int columnIndex) {
        return (long) row.get(columnIndex - 1);
    }

    public float getFloat(int columnIndex) {
        return (float) row.get(columnIndex - 1);
    }

    public double getDouble(int columnIndex) {
        return (double) row.get(columnIndex - 1);
    }

    public byte[] getBytes(int columnIndex) {
        return (byte[]) row.get(columnIndex - 1);
    }

    public Object getObject(int columnIndex) {
        return row.get(columnIndex - 1);
    }

    public RelationalStruct getStruct(int oneBasedPosition) {
        return (RelationalStruct) row.get(oneBasedPosition - 1);
    }

    public RelationalArray getArray(int oneBasedPosition) {
        return (RelationalArray) row.get(oneBasedPosition - 1);
    }
}
