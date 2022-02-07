/*
 * RecordLayerResultSet.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidCursorStateException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.List;

public class RecordLayerResultSet extends AbstractRecordLayerResultSet {
    protected final NestableTuple startKey;
    protected final NestableTuple endKey;
    protected final RecordStoreConnection sourceConnection;

    protected final Scannable scannable;
    protected Continuation continuation;

    protected final QueryProperties scanProperties;

    private final String[] fieldNames;

    private ResumableIterator<KeyValue> currentCursor;

    private KeyValue currentRow;

    public RecordLayerResultSet(Scannable scannable, NestableTuple start, NestableTuple end,
                                RecordStoreConnection sourceConnection, QueryProperties scanProperties,
                                Continuation continuation) {
        this.scannable = scannable;
        this.startKey = start;
        this.endKey = end;
        this.sourceConnection = sourceConnection;
        this.fieldNames = scannable.getFieldNames();
        this.scanProperties = scanProperties;
        this.continuation = continuation;
    }

    @Override
    public boolean next() throws RelationalException {
        currentRow = null;
        if (currentCursor == null) {
            currentCursor = scannable.openScan(sourceConnection.transaction, startKey, endKey, continuation, scanProperties);
        }

        if (!currentCursor.hasNext()) {
            return false;
        }

        currentRow = currentCursor.next();
        return true;
    }

    @Override
    public void close() throws RelationalException {
        if (currentCursor != null) {
            currentCursor.close();
        }
    }

    @Override
    public Object getObject(int position) throws RelationalException {
        if (currentRow == null) {
            throw new InvalidCursorStateException("Cursor was not advanced, or has been exhausted");
        }
        if (supportsMessageParsing()) {
            Message m = ((MessageTuple) currentRow.value()).parseMessage();
            return m.getField(m.getDescriptorForType().findFieldByNumber(position + 1));
        }
        if (position < 0 || position >= (currentRow.keyColumnCount() + currentRow.value().getNumFields())) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }
        Object o;
        if (position < currentRow.keyColumnCount()) {
            o = currentRow.key().getObject(position);
        } else {
            o = currentRow.value().getObject(position - currentRow.keyColumnCount());
        }
        return o;
    }

    @Override
    protected int getPosition(String fieldName) {
        if (supportsMessageParsing()) {
            Message m = parseMessage();
            final List<Descriptors.FieldDescriptor> fields = m.getDescriptorForType().getFields();
            for (Descriptors.FieldDescriptor field : fields) {
                if (field.getName().equalsIgnoreCase(fieldName)) {
                    return field.getIndex();
                }
            }
        } else {
            for (int pos = 0; pos < fieldNames.length; pos++) {
                if (fieldNames[pos] != null && fieldNames[pos].equalsIgnoreCase(fieldName)) {
                    return pos;
                }
            }
        }
        throw new InvalidColumnReferenceException(fieldName);
    }

    @Override
    protected String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public boolean supportsMessageParsing() {
        return currentRow != null && currentRow.value() instanceof MessageTuple;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> M parseMessage() throws OperationUnsupportedException {
        if (!supportsMessageParsing()) {
            throw new OperationUnsupportedException("This ResultSet does not support Message Parsing");
        }
        return ((MessageTuple) currentRow.value()).parseMessage();
    }

    @Override
    public int getNumFields() {
        if (currentRow == null) {
            throw new InvalidCursorStateException("Cursor was not advanced, or has been exhausted");
        }
        if (supportsMessageParsing()) {
            return parseMessage().getDescriptorForType().getFields().size();
        }
        return currentRow.key().getNumFields() + currentRow.value().getNumFields();
    }

    @Override
    public boolean terminatedEarly() {
        return currentCursor.terminatedEarly();
    }

    @Override
    public Continuation getContinuation() {
        if (currentCursor == null) {
            currentCursor = scannable.openScan(sourceConnection.transaction, startKey, endKey, continuation, scanProperties);
        }
        return currentCursor.getContinuation();
    }
}
