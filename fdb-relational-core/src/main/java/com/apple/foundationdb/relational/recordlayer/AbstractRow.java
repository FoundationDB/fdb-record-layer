/*
 * AbstractRow.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;

import com.google.protobuf.Message;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

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
        if (!(o instanceof String)) {
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
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof Row)) {
            return false;
        }
        Row otherTuple = (Row) other;
        final int numFields = this.getNumFields();
        if (numFields != otherTuple.getNumFields()) {
            return false;
        }
        for (int i = 0; i < numFields; i++) {
            try {
                Object lhs = this.getObject(i);
                Object rhs = otherTuple.getObject(i);
                if (lhs instanceof Message) {
                    lhs = new MessageTuple((Message) lhs);
                }
                if (rhs instanceof Message) {
                    rhs = new MessageTuple((Message) rhs);
                }
                if (!Objects.equals(lhs, rhs)) {
                    return false;
                }
            } catch (InvalidColumnReferenceException e) {
                throw e.toUncheckedWrappedException();
            }
        }
        return true;
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
        Row thisRow = this;
        if (prefix.getNumFields() > this.getNumFields()) {
            return false;
        }
        Row truncatedThis = new AbstractRow() {
            @Override
            public int getNumFields() {
                return prefix.getNumFields();
            }

            @Override
            public Object getObject(int position) throws InvalidColumnReferenceException {
                return thisRow.getObject(position);
            }
        };
        return truncatedThis.equals(prefix);
    }
}
