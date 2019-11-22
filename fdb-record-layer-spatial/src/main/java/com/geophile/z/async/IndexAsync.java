/*
 * IndexAsync.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2019 Apple Inc. and the FoundationDB project authors
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

package com.geophile.z.async;

import com.geophile.z.Cursor;
import com.geophile.z.Index;
import com.geophile.z.Record;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous version of {@link com.geophile.z.Index}.
 * @param <RECORD> type for spatial records
 */
@SuppressWarnings({"checkstyle:ClassTypeParameterName", "checkstyle:MethodTypeParameterName", "PMD.GenericsNaming"})
public abstract class IndexAsync<RECORD extends Record>
{
    /**
     * Returns a {@link CursorAsync} that can visit this Index's records.
     * @return A {@link CursorAsync} that can visit this Index's records.
     */
    public abstract CursorAsync<RECORD> cursorAsync();

    /**
     * Returns a {@link com.geophile.z.Record} that can be added to this Index.
     * @return A {@link com.geophile.z.Record} that can be added to this Index.
     */
    public abstract RECORD newRecord();

    /**
     * Returns a {@link com.geophile.z.Record} that can be used as a key to search this Index.
     * @return A {@link com.geophile.z.Record} that can be used as a key to search this Index.
     */
    public RECORD newKeyRecord()
    {
        return newRecord();
    }

    /**
     * Indicates whether records retrieved from this index are stable. A stable record doesn't change
     * as the cursor that produces it advances. A record that is not stable has state tied to the cursor,
     * and may change as the cursor moves.
     * @return true iff records retrieved from this index are stable.
     */
    public abstract boolean stableRecords();

    /**
     * Create an asynchronous index from a synchronous one.
     * @param index a synchronous index
     * @param <RECORD> the type of the indexed records
     * @return an asynchronous wrapper for the given index
     */
    public static <RECORD extends Record> IndexAsync<RECORD> fromSync(final Index<RECORD> index) {
        return new IndexAsync<RECORD>() {
            @Override
            public CursorAsync<RECORD> cursorAsync() {
                final Cursor<RECORD> cursor;
                try {
                    cursor = index.cursor();
                } catch (IOException | InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                return new CursorAsync<RECORD>(this) {
                    @Override
                    public CompletableFuture<RECORD> next() {
                        final RECORD record;
                        try {
                            record = cursor.next();
                        } catch (IOException | InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                        if (record != null) {
                            current(record);
                        }
                        return CompletableFuture.completedFuture(record);
                    }

                    @Override
                    public void goTo(RECORD key) {
                        try {
                            cursor.goTo(key);
                        } catch (IOException | InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                };
            }

            @Override
            public RECORD newRecord() {
                return index.newRecord();
            }

            @Override
            public boolean stableRecords() {
                return index.stableRecords();
            }
        };
    }
}
