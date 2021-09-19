/*
 * KeyValueResultSet.java
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

import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidCursorStateException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.protobuf.Message;

import javax.annotation.Nullable;

/**
 * A ResultSet which holds just a single record.
 */
public class KeyValueResultSet extends AbstractRecordLayerResultSet {
    private final KeyValue keyValue;
    private final String[] fieldNames;

    private boolean nextCalled = false;
    //when true, indicates that all the data is stored in the value of the KeyValue.
    //otherwise, we assume that the key is the value
    private final boolean allDataInValue;

    public KeyValueResultSet(@Nullable KeyValue keyValue, String[] fieldNames, boolean allDataInValue) {
        this.keyValue = keyValue;
        this.fieldNames = fieldNames;
        this.allDataInValue = allDataInValue;
    }

    @Override
    public boolean next() throws RelationalException {
        if (!nextCalled) {
            nextCalled = true;
            return keyValue != null;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws RelationalException {
        //no-op
    }

    @Override
    public Object getObject(int position) throws RelationalException {
        if (!nextCalled) {
            throw new InvalidCursorStateException("Iterator was not advanced");
        }
        if (keyValue == null) {
            throw new RelationalException("empty result set", RelationalException.ErrorCode.UNKNOWN);
        }

        if (position < 0 || position >= (keyValue.keyColumnCount() + keyValue.value().getNumFields())) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }
        if (!allDataInValue && position < keyValue.keyColumnCount()) {
            return keyValue.key().getObject(position);
        } else {
            return keyValue.value().getObject(position);
        }
    }

    @Override
    protected int getPosition(String fieldName) {
        int position = 0;
        for (String field : fieldNames) {
            if (field.equalsIgnoreCase(fieldName)) {
                return position;
            }
            position++;
        }
        return -1;
    }

    @Override
    public boolean supportsMessageParsing() {
        if (!nextCalled) {
            throw new InvalidCursorStateException("Iterator was not advanced");
        }
        if (keyValue == null) {
            throw new RelationalException("empty result set", RelationalException.ErrorCode.UNKNOWN);
        }
        return keyValue.value() instanceof MessageTuple;
    }

    @Override
    public <M extends Message> M parseMessage() throws OperationUnsupportedException {
        if (!supportsMessageParsing()) {
            throw new OperationUnsupportedException("This ResultSet does not support Message Parsing");
        }
        assert keyValue != null: "Programmer error: supportsMessageParsing() should handle non-advanced pointers";
        return ((MessageTuple)keyValue.value()).parseMessage();
    }
}
