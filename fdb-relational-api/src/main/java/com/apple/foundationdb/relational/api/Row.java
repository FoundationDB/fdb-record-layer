/*
 * Row.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

/**
 * Represents a row within the store.
 * <p>
 * The individual fields of the row can themselves be NestableTuples(representing nested
 * data structures). There can also be repeated fields, which is a bit of an extension on
 * standard SQL types.
 * <p>
 * The intent of this class is to represent a single result in a set of results in a generic form.
 */
public interface Row {

    /**
     * Get the number of fields in this row.
     *
     * @return the number of fields in the row.
     */
    int getNumFields();

    /**
     * Get the value at the specified position, as a long.
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as a long.
     * @throws InvalidTypeException     if the field at the position cannot be converted to a long
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    long getLong(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a float.
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as a float.
     * @throws InvalidTypeException     if the field at the position cannot be converted to an float
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    float getFloat(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a double.
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as a double.
     * @throws InvalidTypeException     if the field at the position cannot be converted to a double
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    double getDouble(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a String.
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as a string.
     * @throws InvalidTypeException     if the field at the position cannot be converted to a string
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    String getString(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a byte[].
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as a byte[].
     * @throws InvalidTypeException     if the field at the position cannot be converted to a byte[]
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    byte[] getBytes(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a nested row.
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as a Row
     * @throws InvalidTypeException     if the field at the position cannot be converted to a Row
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    Row getRow(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as a sequence of nested rows.
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as an iterable
     * @throws InvalidTypeException     if the field at the position cannot be converted to a Row
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    Iterable<Row> getArray(int position) throws InvalidTypeException, InvalidColumnReferenceException;

    /**
     * Get the value at the specified position, as an Object.
     *
     * @param position the position in the row
     * @return the value of the row at the specified position, as an Object.
     * @throws InvalidColumnReferenceException if {@code position < 0 } or {@code position >=}{@link #getNumFields()}
     */
    Object getObject(int position) throws InvalidColumnReferenceException;

    /**
     * Checks if this row starts with a given prefix.
     * @param prefix the prefix to check against
     * @return true if this row starts with the correct prefix, false otherwise
     */
    boolean startsWith(Row prefix);

    /**
     * Get a prefix of this row as a row.
     * @param length The length of the prefix
     * @return a prefix Row
     */
    Row getPrefix(int length);
}
