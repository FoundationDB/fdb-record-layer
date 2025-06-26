/*
 * RowArray.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An implementation of a RelationalArray that is materialized, that is, it holds all its elements in a {@link List}. The
 * element can themselves be materialized or not.
 *
 * This class is not thread-safe, and in general should <em>not</em> be used in concurrent environments.
 */
@API(API.Status.EXPERIMENTAL)
public class RowArray implements RelationalArray, EmbeddedRelationalArray {

    private final Supplier<List<Row>> rows;
    private final ArrayMetaData arrayMetaData;

    private final Supplier<Integer> hashCodeSupplier;

    public RowArray(@Nonnull List<?> elements, @Nonnull ArrayMetaData arrayMetaData) {
        this.arrayMetaData = arrayMetaData;
        this.rows = Suppliers.memoize(() -> createIterableRows(elements));
        this.hashCodeSupplier = Suppliers.memoize(this::calculateHashCode);
    }

    private static List<Row> createIterableRows(@Nonnull List<?> elements) {
        int i = 1;
        final List<Row> rows = new ArrayList<>();
        for (var element : elements) {
            rows.add(new ArrayRow(i++, element));
        }
        return rows;
    }

    @Override
    public RelationalResultSet getResultSet(long oneBasedIndex, int count) throws SQLException {
        final var slice = rows.get().stream()
                .skip(oneBasedIndex - 1)
                .limit(count)
                .collect(Collectors.toList());
        final var type = DataType.StructType.from("ARRAY_ROW", List.of(
                DataType.StructType.Field.from("INDEX", DataType.Primitives.INTEGER.type(), 0),
                DataType.StructType.Field.from("VALUE", arrayMetaData.getRelationalDataType().getElementType(), 1)
        ), true);
        return new IteratorResultSet(RelationalStructMetaData.of(type), slice.iterator(), 0);
    }

    @Override
    public int getBaseType() throws SQLException {
        return arrayMetaData.getElementType();
    }

    @Nonnull
    @Override
    public ArrayMetaData getMetaData() throws SQLException {
        return arrayMetaData;
    }

    @Override
    public String toString() {
        return rows.get().stream().map(Objects::toString).collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RowArray)) {
            return false;
        }
        final var otherArrayRow = (RowArray) other;
        if (otherArrayRow == this) {
            return true;
        }
        if (!arrayMetaData.equals(otherArrayRow.arrayMetaData)) {
            return false;
        }

        final var iterator = rows.get().iterator();
        final var otherIterator = otherArrayRow.rows.get().iterator();

        while (iterator.hasNext()) {
            if (!otherIterator.hasNext() || !iterator.next().equals(otherIterator.next())) {
                return false;
            }
        }
        return otherIterator.hasNext();
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int calculateHashCode() {
        return Objects.hash(Arrays.hashCode(rows.get().toArray()), arrayMetaData);
    }
}
