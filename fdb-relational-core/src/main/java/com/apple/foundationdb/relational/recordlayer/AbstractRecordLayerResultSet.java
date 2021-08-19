/*
 * AbstractRecordLayerResultSet.java
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

import com.apple.foundationdb.relational.api.IsolationLevel;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.google.protobuf.Message;

public abstract class AbstractRecordLayerResultSet implements RelationalResultSet {

    @Override
    public IsolationLevel getActualIsolationLevel() {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Override
    public IsolationLevel getRequestedIsolationLevel() {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Override
    public boolean getBoolean(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        Object o = getObject(position);
        if (o == null) {
            return false; //TODO(bfines) return a default value here
        }
        if (!(o instanceof Boolean)) {
            throw new RelationalException("Boolean", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
        }

        return (Boolean) o;
    }

    @Override
    public boolean getBoolean(String fieldName) throws RelationalException {
        return getBoolean(getPosition(fieldName));
    }


    @Override
    public long getLong(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        Object o = getObject(position);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new RelationalException("Long", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
        }

        return ((Number) o).longValue();
    }

    @Override
    public long getLong(String fieldName) throws RelationalException {
        return getLong(getPosition(fieldName));
    }

    @Override
    public float getFloat(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        Object o = getObject(position);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new RelationalException("Long", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
        }

        return ((Number) o).floatValue();
    }

    @Override
    public float getFloat(String fieldName) throws RelationalException {
        return getFloat(getPosition(fieldName));
    }

    @Override
    public double getDouble(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        Object o = getObject(position);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new RelationalException("Long", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
        }

        return ((Number) o).doubleValue();
    }

    @Override
    public double getDouble(String fieldName) throws RelationalException {
        return getDouble(getPosition(fieldName));
    }


    @Override
    public Object getObject(String fieldName) throws RelationalException {
        return getObject(getPosition(fieldName));
    }

    @Override
    public String getString(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        Object o = getObject(position);
        if (o == null) {
            return null;
        }
        if (!(o instanceof String)) {
            throw new RelationalException("String", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
        }

        return (String) o;
    }

    @Override
    public String getString(String fieldName) throws RelationalException {
        return getString(getPosition(fieldName));
    }

    @Override
    public Message getMessage(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        Object o = getObject(position);
        if (o == null) {
            return null;
        }
        if (!(o instanceof Message)) {
            throw new RelationalException("Message", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
        }

        return (Message) o;
    }

    @Override
    public Message getMessage(String fieldName) throws RelationalException {
        return getMessage(getPosition(fieldName));
    }

    @Override
    public Iterable<?> getRepeated(int position) throws RelationalException, ArrayIndexOutOfBoundsException {
        Object o = getObject(position);
        if (o == null) {
            return null;
        }
        if (!(o instanceof Iterable)) {
            throw new RelationalException("Iterable", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
        }

        return (Iterable<?>) o;
    }

    @Override
    public Iterable<?> getRepeated(String fieldName) throws RelationalException {
        return getRepeated(getPosition(fieldName));
    }

    @Override
    public boolean supportsMessageParsing() {
        return false;
    }

    @Override
    public <M extends Message> M parseMessage() throws RelationalException {
        throw new UnsupportedOperationException("Does not support message parsing");
    }

    /* ****************************************************************************************************************/
    /* private helper methods*/
    protected abstract int getPosition(String fieldName);
}
