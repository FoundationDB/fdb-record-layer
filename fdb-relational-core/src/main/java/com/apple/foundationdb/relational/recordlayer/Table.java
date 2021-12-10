/*
 * Table.java
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

import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Set;


@ConnectionScoped
public interface Table extends Scannable, AutoCloseable {

    @Nonnull
    DatabaseSchema getSchema();

    //TODO(bfines) I'm not sure if this is the right place to put this logic
    // on the one hand, it feels natural. On the other hand, would is expose too much
    // to external users of the Catalog? Perhaps not, since it still wraps out the
    // correct mutation behavior
    // and what about scans/gets? Should they go here too?

    boolean deleteRecord(@Nonnull NestableTuple key) throws RelationalException;

    //TODO(bfines) should we really use a Protobuf here? Or a KV pair instead? For now, Message will work
    boolean insertRecord(@Nonnull Message message) throws RelationalException;

    Set<Index> getAvailableIndexes();

    @Override
    void close() throws RelationalException;

    String getName();

    /**
     * Get the metadata descriptor for this table.
     *
     * @return the metadata descriptor for this table.
     */
    @Nonnull TableMetaData getMetaData();
}
