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

import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

/**
 * An implementation of a RelationalArray using a collection of {@link Row} objects as backing storage.
 *
 * In Relational's implementation of JDBC, an Array is functionally a materialized ResultSet, which makes it
 * much more convenient for us to implement Arrays using the same ResultSet logic that we currently possess,
 * almost all of which relies on using an iteration of Row objects. To that end, this class provides support
 * for that natural usage pattern.
 *
 * This class is not thread-safe, and in general should <em>not</em> be used in concurrent environments.
 */
public class RowArray extends RelationalArray {
    private final Iterable<Row> rows;
    private final StructMetaData arrayMetaData;

    public RowArray(@Nonnull Iterable<Row> rows,
                    @Nonnull StructMetaData arrayMetaData) {
        this.rows = rows;
        this.arrayMetaData = arrayMetaData;
    }

    @Override
    public String getBaseTypeName() {
        return SqlTypeSupport.getSqlTypeName(getBaseType());
    }

    @Override
    public int getBaseType() {
        /*
         * Confusion alert!
         *
         * The JDBC API for array uses this method to refer to "the JDBC type of the elements in the array
         * designated by this Array object."--i.e. this is the Sql Type for the contents of the array.
         * In Relational, the contents of an Array is _always_ a Struct (from the JDBC API's point view), mostly
         * because it makes it much easier for us to implement the nested ResultSet implementation.
         */
        return Types.STRUCT;
    }

    /**
     * Retrieves a slice of the SQL <code>ARRAY</code>
     * value designated by this <code>Array</code> object, beginning with the
     * specified <code>index</code> and containing up to <code>count</code>
     * successive elements of the SQL array.  This method uses the type map
     * associated with the connection for customizations of the type mappings.
     *
     * Note: In Relational, this method will always return an array of Struct types. This is
     * _technically_ in violation of the SQL spec, but it allows for a user experience which
     * is more consistent w.r.t how metadata is available and handled.
     *
     *
     * @param oneBasedIndex index the array index of the first element to retrieve;
     *              the first element is at index 1
     * @param count the number of successive SQL array elements to retrieve
     * @return an array containing up to <code>count</code> consecutive elements
     * of the SQL array, beginning with element <code>index</code>
     * @exception SQLException if an error occurs while attempting to
     * access the array
     */
    @Override
    public Object getArray(long oneBasedIndex, int count) throws SQLException {
        final int columnCount = arrayMetaData.getColumnCount();

        try {
            return StreamSupport.stream(rows.spliterator(), false)
                    .skip(oneBasedIndex - 1)
                    .limit(count)
                    .map(row -> {
                        RowStruct struct = new ImmutableRowStruct(row, arrayMetaData);
                        Object[] castRow = new Object[columnCount];
                        for (int i = 0; i < columnCount; i++) {
                            try {
                                castRow[i] = struct.getObject(i + 1);
                            } catch (SQLException e) {
                                throw new RelationalException(e).toUncheckedWrappedException();
                            }
                        }
                        return castRow;
                    }).toArray();
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap().toSqlException();
        }
    }

    @Override
    public ResultSet getResultSet(long oneBasedIndex, int count) {
        List<Row> dataStream = StreamSupport.stream(rows.spliterator(), false)
                .skip(oneBasedIndex - 1)
                .limit(count)
                .collect(Collectors.toList());

        return new IteratorResultSet(arrayMetaData, dataStream.iterator(), 0);
    }
}
