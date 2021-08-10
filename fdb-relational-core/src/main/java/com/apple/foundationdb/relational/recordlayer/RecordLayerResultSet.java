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
import com.apple.foundationdb.relational.api.Scanner;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Scannable;
import com.apple.foundationdb.relational.api.RelationalException;
import com.google.protobuf.Message;

public class RecordLayerResultSet extends AbstractRecordLayerResultSet {
    protected final NestableTuple startKey;
    protected final NestableTuple endKey;
    protected final RecordStoreConnection sourceConnection;
    protected final Options scanOptions;

    protected final Scannable scannable;
    protected Continuation lastContinuation = null;

    public RecordLayerResultSet(Scannable scannable, NestableTuple start,NestableTuple end,
                                        RecordStoreConnection sourceConnection,Options scanOptions) {
        this.scannable = scannable;
        this.startKey = start;
        this.endKey = end;
        this.sourceConnection = sourceConnection;
        this.scanOptions = scanOptions;
        this.fieldNames = scannable.getFieldNames();
    }

    private final String[] fieldNames;

    private Scanner<KeyValue> currentCursor;

    private KeyValue currentRow;


    @Override
    public boolean next() throws RelationalException {
        currentRow = null;
        if (currentCursor == null) {
            currentCursor = scannable.openScan(sourceConnection.transaction, startKey,endKey, scanOptions.withOption(OperationOption.continuation(lastContinuation)));
        }
        if(!currentCursor.hasNext()){
            return false;
        }

        currentRow = currentCursor.next();
        lastContinuation = currentCursor.continuation();
        return true;
    }

    @Override
    public void close() throws RelationalException {
        if (currentCursor != null) {
            currentCursor.close();
        }
    }

    @Override
    public Object getObject(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        if(currentRow==null){
            throw new IllegalStateException("Iterator was not advanced or has terminated");
        }
        if(position <0 || position >= (currentRow.keyColumnCount()+currentRow.value().getNumFields())){
            throw new ArrayIndexOutOfBoundsException();
        }
        Object o;
        if(position < currentRow.keyColumnCount()){
            o = currentRow.key().getObject(position);
        }else{
            o = currentRow.value().getObject(position-currentRow.keyColumnCount());
        }
        return o;
    }

    @Override
    protected int getPosition(String fieldName) {
        for(int pos = 0; pos < fieldNames.length; pos++){
            if(fieldNames[pos].equalsIgnoreCase(fieldName)){
                return pos;
            }
        }
        throw new RelationalException(fieldName,RelationalException.ErrorCode.INVALID_COLUMN_REFERENCE);
    }

    @Override
    public boolean supportsMessageParsing() {
        return currentRow.keyColumnCount()==0 && currentRow.value().getObject(0) instanceof Message;
    }

    @Override
    public <M extends Message> M parseMessage() throws RelationalException {
        if(!supportsMessageParsing()){
            throw new UnsupportedOperationException("This ResultSet does not support Message Parsing");
        }
        return (M)currentRow.value().getObject(0);
    }
}
