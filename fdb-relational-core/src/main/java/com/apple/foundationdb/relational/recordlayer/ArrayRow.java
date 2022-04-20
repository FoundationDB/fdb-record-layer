/*
 * ArrayRow.java
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

import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

@SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "Intentionally exposed for performance reasons")
public class ArrayRow extends AbstractRow {
    private final Object[] data;

    public ArrayRow(Object[] data) {
        this.data = data;
    }

    @Override
    public int getNumFields() {
        return data.length;
    }

    @Override
    public Object getObject(int position) throws InvalidColumnReferenceException {
        if (position < 0 || position >= data.length) {
            throw new InvalidColumnReferenceException(Integer.toString(position));
        }
        return data[position];
    }
}
