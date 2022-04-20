/*
 * TabularToProtobufParser.java
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

import com.apple.foundationdb.relational.api.RelationalResultSet;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

/**
 * Converts tabular-structured normalized data to a nested Protobuf.
 */
class TabularToProtobufParser {
    private final Descriptors.Descriptor topLevelDescriptor;

    public TabularToProtobufParser(Descriptors.Descriptor topLevelDescriptor) {
        this.topLevelDescriptor = topLevelDescriptor;
    }

    public Collection<Message> parse(RelationalResultSet rrs) throws SQLException {
        final ResultSetMetaData metaData = rrs.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            System.out.printf("%s , ", metaData.getTableName(i) + "." + metaData.getColumnLabel(i));
        }
        System.out.println();

        /*
         * The tabular contents are of the form
         *  <Top_type>.RECORD_ID, <Top_type>.<columns>, <Nested_type>.<columns>...
         */
        Collection<Message> retList = new ArrayList<>();
        TypeNodeFactory factory = new TypeNodeFactory();
        Queue<Row> queue = new ResultSetQueue(rrs);

        final TypeNode typeNode = factory.newNode(topLevelDescriptor);
        Message next;
        while ((next = typeNode.parseType(queue)) != null) {
            retList.add(next);
        }
        return retList;
    }

    static class ResultSetQueue extends AbstractQueue<Row> {
        private final RelationalResultSet rrs;
        private Row currentRow;

        public ResultSetQueue(RelationalResultSet rrs) {
            this.rrs = rrs;
        }

        @Override
        public Iterator<Row> iterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return peek() != null;
                }

                @Override
                public Row next() {
                    return poll();
                }
            };
        }

        @Override
        public boolean isEmpty() {
            return peek() == null;
        }

        @Override
        public int size() {
            return -1;
        }

        @Override
        public boolean offer(Row row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Row poll() {
            Row n = currentRow;
            currentRow = null;
            return n;
        }

        @Override
        public Row peek() {
            if (currentRow == null) {
                try {
                    if (!rrs.next()) {
                        return null;
                    }
                    makeRow(rrs);
                } catch (SQLException se) {
                    throw new RuntimeException(se);
                }
            }
            return currentRow;
        }

        private void makeRow(RelationalResultSet rrs) throws SQLException {
            final ResultSetMetaData metaData = rrs.getMetaData();
            Map<String, Object> rowData = new HashMap<>();
            String[] columnNames = new String[metaData.getColumnCount()];
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String name = metaData.getColumnName(i);
                String fullName = metaData.getTableName(i) + "." + name;
                Object o = rrs.getObject(i);
                if (o != null) {
                    rowData.put(fullName, o);
                }
                columnNames[i - 1] = name;
            }
            currentRow = new MapRow(rowData, columnNames);
        }
    }

    static class TypeNodeFactory {

        public TypeNode newNode(Descriptors.Descriptor typeDescriptor) {
            var columnNameTransform = new Function<Descriptors.FieldDescriptor, String>() {
                @Override
                public String apply(Descriptors.FieldDescriptor fieldDescriptor) {
                    return fieldDescriptor.getContainingType().getName() + "." + fieldDescriptor.getName();
                }
            };

            return new TypeNode(this, typeDescriptor, columnNameTransform);
        }
    }

    static class TypeNode {
        private final TypeNodeFactory nestedTypeFactory;
        private final Descriptors.Descriptor typeDescriptor;
        private final Function<Descriptors.FieldDescriptor, String> columnNameTransform;

        public TypeNode(TypeNodeFactory nestedTypeFactory,
                        Descriptors.Descriptor typeDescriptor,
                        Function<Descriptors.FieldDescriptor, String> columnNameTransform) {
            this.nestedTypeFactory = nestedTypeFactory;
            this.typeDescriptor = typeDescriptor;
            this.columnNameTransform = columnNameTransform;
        }

        public Message parseType(Queue<Row> rows) throws SQLException {
            //read all the rows whose RECORD_ID field matches ours
            Queue<Row> ourData = new LinkedList<>();
            Row nextRow;
            Object lastId = null;
            while ((nextRow = rows.peek()) != null) {
                Object id = nextRow.getObject(typeDescriptor.getName().toUpperCase(Locale.ROOT) + ".RECORD_ID");
                if (id == null) {
                    return null; //this means there are no data point for this type
                }
                if (lastId == null) {
                    //we are starting iteration
                    lastId = id;
                }
                if (!id.equals(lastId)) {
                    break;
                } else {
                    ourData.add(nextRow);
                    rows.poll(); //remove this from the iterator
                }
            }
            if (ourData.isEmpty()) {
                //there is no data to return
                return null;
            }

            Message.Builder dm = DynamicMessage.newBuilder(typeDescriptor);

            /*
             * We populate the non-repeated fields first. This data
             * should be the same for all the rows that we process, so we just
             * take the first row to deal with it.
             */
            Row mainRow = ourData.peek();
            List<Descriptors.FieldDescriptor> repeatedFields = new LinkedList<>();
            for (Descriptors.FieldDescriptor fd : typeDescriptor.getFields()) {
                if (fd.isRepeated()) {
                    repeatedFields.add(fd);
                } else if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    //this is a nested type, so recursively parse that and add it to us
                    TypeNode newType = nestedTypeFactory.newNode(fd.getMessageType());
                    //non-repeated so it's value will be the same throughout our data, so just feed it one item
                    Queue<Row> theRow = new LinkedList<>();
                    theRow.add(mainRow);
                    Message nestedType = newType.parseType(theRow);
                    dm.setField(fd, nestedType);
                } else {
                    String colName = columnNameTransform.apply(fd);
                    Object o = mainRow.getObject(colName);
                    if (o != null) {
                        //TODO(bfines) this should do default values
                        dm.setField(fd, o);
                    }
                }
            }

            /*
             * Now we process the repeated data. Because each repeated field may (or may not) be repeated,
             * and may (or may not) contain their own repeated structures, we feed each subtype all the rows that
             * we have
             */
            for (Descriptors.FieldDescriptor fd : repeatedFields) {
                Queue<Row> subQueue = new LinkedList<>(ourData);
                if (fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    TypeNode type = nestedTypeFactory.newNode(fd.getMessageType());
                    Message message;
                    while ((message = type.parseType(subQueue)) != null) {
                        dm.addRepeatedField(fd, message);
                    }
                } else {
                    //This is not a message type, so we just look for the field <table>_<field> value in the query
                    throw new UnsupportedOperationException("Not Implemented in the Relational layer");
                }
            }
            return dm.build();
        }
    }
}
