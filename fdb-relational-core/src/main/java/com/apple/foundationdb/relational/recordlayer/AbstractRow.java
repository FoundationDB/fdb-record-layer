/*
 * AbstractRow.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.lang.Math.min;

public abstract class AbstractRow implements Row {

    @Override
    public long getLong(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }
        Object o = getObject(position);
        if (!(o instanceof Number)) {
            throw new InvalidTypeException("Value <" + o + "> cannot be cast to a scalar type");
        }
        return ((Number) o).longValue();
    }

    @Override
    public float getFloat(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }

        Object o = getObject(position);
        if (!(o instanceof Number)) {
            throw new InvalidTypeException("Value <" + o + "> cannot be cast to a float type");
        }
        return ((Number) o).floatValue();
    }

    @Override
    public double getDouble(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }

        Object o = getObject(position);
        if (!(o instanceof Number)) {
            throw new InvalidTypeException("Value <" + o + "> cannot be cast to a double type");
        }
        return ((Number) o).doubleValue();
    }

    @Override
    public String getString(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }

        Object o = getObject(position);
        if (o instanceof Enum<?>) {
            return ((Enum<?>) o).name();
        } else if (o instanceof Descriptors.EnumValueDescriptor) {
            return ((Descriptors.EnumValueDescriptor) o).getName();
        } else if (!(o instanceof String)) {
            throw new InvalidTypeException("Value <" + o + "> cannot be cast to a String");
        }
        return (String) o;
    }

    @Override
    public byte[] getBytes(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }

        Object o = getObject(position);
        if (!(o instanceof byte[])) {
            throw new InvalidTypeException("Value <" + o + "> cannot be cast to a byte[]");
        }
        return (byte[]) o;
    }

    @Override
    public Row getRow(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }

        Object o = getObject(position);
        if (o instanceof Row) {
            return (Row) o;
        } else if (o instanceof Tuple) {
            return new FDBTuple((Tuple) o);
        } else if (o instanceof Message) {
            return new MessageTuple((Message) o);
        } else {
            throw new InvalidTypeException("Value <" + o + "> cannot be cast to a tuple");
        }
    }

    @Override
    public Iterable<Row> getArray(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }

        Object o = getObject(position);

        if (o instanceof Iterable) {
            return StreamSupport.stream(((Iterable<?>) o).spliterator(), false).map(obj -> {
                if (obj instanceof Row) {
                    return (Row) obj;
                } else if (obj instanceof Tuple) {
                    return new FDBTuple((Tuple) obj);
                } else {
                    return new ValueTuple(obj);
                }
            }).collect(Collectors.toList());
        } else {
            throw new InvalidTypeException("Object <" + o + "> cannot be converted to a repeated type");
        }
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        if (!(other instanceof Row)) {
            return false;
        }
        final Row otherTuple = (Row) other;
        final int numFields = this.getNumFields();
        if (numFields != otherTuple.getNumFields()) {
            return false;
        }
        for (int i = 0; i < numFields; i++) {
            try {
                final var lhs = this.getObject(i);
                final var rhs = otherTuple.getObject(i);
                if (lhs instanceof Collection && rhs instanceof Collection) {
                    final var lhsCollection = (Collection<?>) lhs;
                    final var rhsCollection = (Collection<?>) rhs;
                    if (lhsCollection.size() != rhsCollection.size()) {
                        return false;
                    }
                    return Streams.zip(lhsCollection.stream(), rhsCollection.stream(), AbstractRow::areEqual).allMatch(t -> t);
                }
                if (!areEqual(lhs, rhs)) {
                    return false;
                }
            } catch (InvalidColumnReferenceException e) {
                throw e.toUncheckedWrappedException();
            }
        }
        return true;
    }

    private static boolean areEqual(@Nullable final Object lhs, @Nullable final Object rhs) {
        if (lhs == null && rhs == null) {
            return true;
        }
        if (lhs == null || rhs == null) {
            return false;
        }
        final var rhsMessage = rhs instanceof Message ? new MessageTuple((Message) rhs) : rhs;
        final var lhsMessage = lhs instanceof Message ? new MessageTuple((Message) lhs) : lhs;
        return Objects.equals(lhsMessage, rhsMessage);
    }

    @Override
    public int hashCode() {
        if (this.getNumFields() == 0) {
            return 0;
        }
        return Objects.hash(IntStream.range(0, getNumFields()).mapToObj(position -> {
            try {
                return getObject(position);
            } catch (InvalidColumnReferenceException e) {
                throw e.toUncheckedWrappedException();
            }
        }).toArray());
    }

    @Override
    public boolean startsWith(Row prefix) {
        if (prefix.getNumFields() > this.getNumFields()) {
            return false;
        }

        return getPrefix(prefix.getNumFields()).equals(prefix);
    }

    @Override
    public Row getPrefix(int length) {
        Row thisRow = this;
        return new AbstractRow() {
            @Override
            public int getNumFields() {
                return min(thisRow.getNumFields(), length);
            }

            @Override
            public Object getObject(int position) throws InvalidColumnReferenceException {
                if (position > getNumFields()) {
                    throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
                }
                return thisRow.getObject(position);
            }
        };
    }

    @Override
    public String toString() {
        try {
            int numCols = getNumFields();
            StringBuilder sb = new StringBuilder("(");
            boolean isFirst = true;
            for (int i = 0; i < numCols; i++) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    sb.append(",");
                }
                Object colValue = getObject(i);
                if (colValue instanceof Object[]) {
                    sb.append("Array").append(Arrays.toString((Object[]) colValue));
                } else if (colValue instanceof Collection) {
                    String fieldVals = ((Collection<?>) colValue).stream()
                            .map(obj -> obj == null ? "NULL" : obj.toString())
                            .collect(Collectors.joining(", "));
                    sb.append("Array[").append(fieldVals).append("]");
                } else if (colValue instanceof byte[]) {
                    sb.append("ByteArray[").append(ByteArrayUtil2.toHexString((byte[]) colValue)).append("]");
                } else {
                    sb.append(colValue);
                }
            }
            return sb.append(")").toString();
        } catch (RelationalException ve) {
            throw ve.toUncheckedWrappedException();
        }
    }
}
