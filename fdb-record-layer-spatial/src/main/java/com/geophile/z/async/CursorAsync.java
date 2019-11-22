/*
 * CursorAsync.java
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

import com.geophile.z.Record;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous version of {@link com.geophile.z.Cursor}.
 * @param <RECORD> type for spatial records
 */
@SuppressWarnings({"checkstyle:ClassTypeParameterName", "PMD.GenericsNaming"})
public abstract class CursorAsync<RECORD extends Record>
{
    // Object state

    private final boolean stableRecords;
    private RECORD current;
    private State state = State.NEVER_USED;

    // CursorAsync interface

    /**
     * <ul>
     * <li><b>If the Cursor has just been created:</b>
     *     The result of calling this method is undefined.
     *
     * <li><b>If the Cursor has just been positioned using {@link #goTo(Record)}:</b>
     *     This method moves the Cursor to the
     *     {@link com.geophile.z.Record} with the key passed to goTo, or to the smallest
     *     {@link com.geophile.z.Record} whose key is greater than that key. If the key
     *     is greater than that largest key in the index, then the Cursor is closed and null is returned.
     *
     * <li><b>If the Cursor has just been accessed using next():</b>
     *     This method moves the Cursor to the {@link com.geophile.z.Record}
     *     with the next larger key, or to null if the Cursor was already positioned at the last
     *     {@link com.geophile.z.Record} of the index.
     * </ul>
     * @return A future completing to the {@link com.geophile.z.Record} at the new Cursor position, or null if the Cursor
     * was moved past the last record.
     */
    public abstract CompletableFuture<RECORD> next();

    /**
     * Position the Cursor at the {@link com.geophile.z.Record} with the given key. If there is
     * no such record, then the Cursor position is "between" records, and a call to {@link #next()}
     * will position the Cursor at one of the bounding records.
     * @param key The key to search for.
     */
    public abstract void goTo(RECORD key);

    /**
     * Mark the Cursor as no longer usable. Subsequent calls to {@link #goTo(Record)} or {@link #next()}
     * will have undefined results.
     */
    public void close()
    {
        state = State.DONE;
        current = null;
    }

    // For use by subclasses

    protected final RECORD current()
    {
        return current;
    }

    protected final void current(RECORD record)
    {
        assert state != State.DONE;
        if (stableRecords) {
            current = record;
        } else {
            record.copyTo(current);
        }
    }

    protected State state()
    {
        return state;
    }

    protected void state(State newState)
    {
        assert newState != State.DONE;
        state = newState;
    }

    protected CursorAsync(IndexAsync<RECORD> indexAsync)
    {
        stableRecords = indexAsync.stableRecords();
        current = stableRecords ? null : indexAsync.newRecord();
    }

    // Inner classes

    protected enum State
    {
        // The cursor has been created, but has never been used to retrieve a record. If the key used to create
        // the cursor is present, then next() will retrieve the associated record. This state
        // is also used when a cursor is repositioned using goTo().
        NEVER_USED,

        // The cursor has been created and used to retrieve at least one record. next() moves the
        // cursor before retrieving a record.
        IN_USE,

        // The cursor has returned all records. A subsequent call to next() will return null.
        DONE
    }
}
