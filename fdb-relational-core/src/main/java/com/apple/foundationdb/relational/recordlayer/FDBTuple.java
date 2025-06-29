/*
 * FDBTuple.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class FDBTuple extends AbstractRow {
    private Tuple tuple;

    public FDBTuple(Tuple tuple) {
        this.tuple = tuple;
    }

    void setTuple(Tuple t) {
        this.tuple = t;
    }

    @Override
    public int getNumFields() {
        return tuple.size();
    }

    @Override
    public Object getObject(int position) throws InvalidColumnReferenceException {
        if (position < 0 || position >= tuple.size()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }
        return tuple.get(position);
    }

    @Override
    public Row getRow(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= tuple.size()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }
        try {
            return new FDBTuple(tuple.getNestedTuple(position));
        } catch (ClassCastException cce) {
            throw new InvalidTypeException("Object <" + tuple.get(position) + "> cannot be converted to a Tuple type", cce);
        }
    }

    @Override
    public Iterable<Row> getArray(int position) throws InvalidTypeException, InvalidColumnReferenceException {
        if (position < 0 || position >= tuple.size()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }
        try {
            final List<Object> nestedList = tuple.getNestedList(position);
            return nestedList.stream().map(obj -> {
                if (obj instanceof Tuple) {
                    return new FDBTuple((Tuple) obj);
                } else {
                    return new ValueTuple(obj);
                }
            }).collect(Collectors.toList());
        } catch (ClassCastException cce) {
            throw new InvalidTypeException("Object <" + tuple.get(position) + "> cannot be converted to an iterable type", cce);
        }
    }

    Tuple fdbTuple() {
        return tuple;
    }

    @Override
    public String toString() {
        return tuple.toString();
    }

    /**
     * Copy an FDB tuple from another type of tuple.
     *
     * @param copy the tuple to copy
     */
    @Nonnull
    public static FDBTuple fromRow(@Nonnull Row copy) throws RelationalException {
        List<Object> items = new ArrayList<>(copy.getNumFields());
        for (int i = 0; i < copy.getNumFields(); i++) {
            items.add(copy.getObject(i));
        }
        Tuple t = Tuple.fromList(items);
        return new FDBTuple(t);
    }
}
