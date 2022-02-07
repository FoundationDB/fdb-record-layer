/*
 * NestableTuple.java
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

import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;

import java.util.Objects;

/**
 * Represents a tuple within the store.
 * <p>
 * The individual fields of the tuple can themselves be NestableTuples(representing nested
 * data structures). There can also be repeated fields, which is a bit of an extension on
 * standard SQL types.
 * <p>
 * The intent of this class is to represent a single result in a set of results in a generic form.
 */
public interface NestableTuple {

    /**
     * Get the number of fields in this tuple.
     *
     * @return the number of fields in the tuple.
     */
    int getNumFields();

    /**
     * Get the value at the specified position, as a long.
     *
     * @param position the position in the tuple
     * @return the value of the tuple at the specified position, as a long.
     * @throws InvalidTypeException     if the field at the position cannot be converted to a long
     * @throws IllegalArgumentException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    long getLong(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a float.
     *
     * @param position the position in the tuple
     * @return the value of the tuple at the specified position, as a float.
     * @throws InvalidTypeException     if the field at the position cannot be converted to an float
     * @throws IllegalArgumentException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    float getFloat(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a double.
     *
     * @param position the position in the tuple
     * @return the value of the tuple at the specified position, as a float.
     * @throws InvalidTypeException     if the field at the position cannot be converted to a double
     * @throws IllegalArgumentException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    double getDouble(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a String.
     *
     * @param position the position in the tuple
     * @return the value of the tuple at the specified position, as a string.
     * @throws InvalidTypeException     if the field at the position cannot be converted to a string
     * @throws IllegalArgumentException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    String getString(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a byte[].
     *
     * @param position the position in the tuple
     * @return the value of the tuple at the specified position, as a byte[].
     * @throws InvalidTypeException     if the field at the position cannot be converted to a byte[]
     * @throws IllegalArgumentException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    byte[] getBytes(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a nested tuple.
     *
     * @param position the position in the tuple
     * @return the value of the tuple at the specified position, as a NestableTuple
     * @throws InvalidTypeException     if the field at the position cannot be converted to a NestableTuple
     * @throws IllegalArgumentException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    NestableTuple getTuple(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as an sequence of nested tuples.
     *
     * @param position the position in the tuple
     * @return the value of the tuple at the specified position, as an iterable
     * @throws InvalidTypeException     if the field at the position cannot be converted to a NestableTuple
     * @throws IllegalArgumentException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    Iterable<NestableTuple> getArray(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    Object getObject(int position);

    default boolean equalTo(NestableTuple other) {
        if (other == null) {
            return false;
        }
        final int numFields = this.getNumFields();
        if (numFields != other.getNumFields()) {
            return false;
        }
        for (int i = 0; i < numFields; i++) {
            if (!Objects.equals(this.getObject(i), other.getObject(i))) {
                return false;
            }
        }
        return true;
    }
}
