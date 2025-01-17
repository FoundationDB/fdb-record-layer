/*
 * Table.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;


@ConnectionScoped
public interface Table extends DirectScannable, AutoCloseable {

    @Nonnull
    DatabaseSchema getSchema();

    //TODO(bfines) I'm not sure if this is the right place to put this logic
    // on the one hand, it feels natural. On the other hand, would is expose too much
    // to external users of the Catalog? Perhaps not, since it still wraps out the
    // correct mutation behavior
    // and what about scans/gets? Should they go here too?

    boolean deleteRecord(@Nonnull Row key) throws RelationalException;

    void deleteRange(Map<String, Object> prefix) throws RelationalException;

    /**
     * Insert message content.
     * @param message What to insert.
     * @param replaceOnDuplicate Where to replace if duplicate insert.
     * @return Whether insert was successful or not.
     * @throws RelationalException Thrown if record exists already or if error on insert.
     * @deprecated since 01/18/2023; use {@link #insertRecord(RelationalStruct, boolean)} instead.
     */
    @Deprecated
    boolean insertRecord(@Nonnull Message message, boolean replaceOnDuplicate) throws RelationalException;

    /**
     * Replaces {@link #insertRecord(Message, boolean)} encapsulating protobuf usage.
     * @param insert What to insert.
     * @param replaceOnDuplicate Where to replace if duplicate insert.
     * @return Whether insert was successful or not.
     * @throws RelationalException Thrown if record exists already or if error on insert.
     */
    boolean insertRecord(@Nonnull RelationalStruct insert, boolean replaceOnDuplicate) throws RelationalException;

    Set<Index> getAvailableIndexes() throws RelationalException;

    @Override
    void close() throws RelationalException;

    @Nonnull
    @Override
    String getName();

    /**
     * Directly validate that this table is suitable for the operations desired, using the specified
     * control options.
     *
     * If this table is not valid for the options specified, a {@link RelationalException} is thrown.
     *
     * @param options the options to use.
     * @throws RelationalException if the table is invalid for these options.
     */
    void validateTable(@Nonnull Options options) throws RelationalException;
}
