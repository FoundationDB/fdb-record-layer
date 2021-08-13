/*
 * RelationalResultSet.java
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

import com.google.protobuf.Message;

/**
 * Represents the results of a query against the system.
 */
public interface RelationalResultSet extends AutoCloseable {
    //TODO(bfines) at some point, we will need to make this subinterface the java.sql.ResultSet
    //for full SQL support.

    boolean next() throws RelationalException;

    @Override
    void close() throws RelationalException;

    /**
     * Get the actual isolation level used in the scan.
     *
     * @return the actual isolation level used in the scan. If the timeout policy is set to
     * {@link OperationOption.TimeoutPolicy#DOWNGRADE_ISOLATION}, then
     * this isolation level <em>may</em> be {@link IsolationLevel#READ_COMMITTED},
     * even if the query was started with a higher isolation level.
     */
    IsolationLevel getActualIsolationLevel();

    /**
     * Get the isolation level at the start of the read operation.
     *
     * @return the isolation level that was requested when the scan was initiated. This may
     * differ from {@link #getActualIsolationLevel()}
     */
    IsolationLevel getRequestedIsolationLevel();

    boolean getBoolean(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    boolean getBoolean(String fieldName) throws RelationalException;

    long getLong(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    long getLong(String fieldName) throws RelationalException;

    float getFloat(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    float getFloat(String fieldName) throws RelationalException;

    double getDouble(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    double getDouble(String fieldName) throws RelationalException;

    Object getObject(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    Object getObject(String fieldName) throws RelationalException;

    String getString(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    String getString(String fieldName) throws RelationalException;

    /**
     * Return this value as a protobuf message (if possible).
     *
     * @param position the position to get the value from
     * @return the object at the specified position, as a protobuf Message
     * @throws RelationalException if something goes wrong
     * @throws ArrayIndexOutOfBoundsException if the specified position is invalid
     */
    Message getMessage(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    Message getMessage(String fieldName) throws RelationalException;

    Iterable<?> getRepeated(int position) throws RelationalException,ArrayIndexOutOfBoundsException;

    Iterable<?> getRepeated(String fieldName) throws RelationalException;

    /**
     * Determine if this result set support directly returning protobuf objects.
     *
     * @return {@code true} if rows can be parsed directly to protobuf message objects, {@code false} otherwise.
     */
    boolean supportsMessageParsing();

    /**
     * Parse the current row as a protobuf message.
     *
     * @param <M> the type of Message to parse.
     * @return the current row as a protobuf message object.
     * @throws RelationalException if something goes wrong
     * @throws InvalidTypeException if the Row's data type is not {@code M}
     */
    <M extends Message> M parseMessage() throws RelationalException, InvalidTypeException;
}

