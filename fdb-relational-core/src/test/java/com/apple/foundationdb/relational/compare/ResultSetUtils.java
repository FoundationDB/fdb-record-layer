/*
 * ResultSetUtils.java
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

package com.apple.foundationdb.relational.compare;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.RelationalResultSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@API(API.Status.EXPERIMENTAL)
public final class ResultSetUtils {

    public static Iterator<Row> resultSetIterator(RelationalResultSet rrs) throws SQLException {
        return new ResultSetIterator(rrs);
    }

    public static Stream<Row> streamResultSet(RelationalResultSet rrs) throws SQLException {
        final Spliterator<Row> iter = Spliterators.spliteratorUnknownSize(new ResultSetIterator(rrs), 0);
        return StreamSupport.stream(iter, false);
    }

    public static Collection<Row> materializeResultSet(RelationalResultSet rrs) throws SQLException {
        try {
            return streamResultSet(rrs).map(r -> {
                //here we make a copy of the Row because we know the internal iterator is a duplicative entry
                try {
                    return ((ResultSetIterator.ResultSetRow) r).toMapRow();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        } catch (RuntimeException re) {
            //unwrap the SQLException so that it carries throw properly
            if (re.getCause() instanceof SQLException) {
                throw ((SQLException) re.getCause());
            }
            throw re;
        }
    }

    private static final class ResultSetIterator implements Iterator<Row> {
        private final RelationalResultSet resultSet;
        private final Row currentRow;
        private boolean nextCalled;
        private boolean hasNext;

        private ResultSetIterator(RelationalResultSet resultSet) throws SQLException {
            this.resultSet = resultSet;
            this.currentRow = new ResultSetRow(resultSet);
        }

        @Override
        public boolean hasNext() {
            if (nextCalled) {
                return hasNext;
            }

            //this will advance the contents of `currentRow`
            try {
                hasNext = resultSet.next();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return hasNext;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            nextCalled = false;
            return currentRow;
        }

        private static final class ResultSetRow implements Row {
            private final RelationalResultSet resultSet;
            private final int numColumns;

            private ResultSetRow(RelationalResultSet resultSet) throws SQLException {
                this.resultSet = resultSet;
                this.numColumns = resultSet.getMetaData().getColumnCount();
            }

            @Override
            public Object getObject(String name) throws SQLException {
                return resultSet.getObject(name);
            }

            @Override
            public Object getObject(int position) throws SQLException {
                return resultSet.getObject(position);
            }

            @Override
            public int getNumColumns() {
                return numColumns;
            }

            public MapRow toMapRow() throws SQLException {
                Map<String, Object> data = new HashMap<>();
                String[] columnNames = new String[numColumns];
                final ResultSetMetaData metaData = resultSet.getMetaData();
                for (int i = 1; i <= numColumns; i++) {
                    String fullName = metaData.getTableName(i) + metaData.getColumnName(i);
                    columnNames[i - 1] = fullName;
                    data.put(fullName, resultSet.getObject(i));
                }
                return new MapRow(data, columnNames);
            }
        }
    }

    private ResultSetUtils() {
    }
}
