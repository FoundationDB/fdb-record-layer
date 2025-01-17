/*
 * Row.java
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

package com.apple.foundationdb.relational.compare;

import java.sql.SQLException;

interface Row {
    Object getObject(String name) throws SQLException;

    Object getObject(int position) throws SQLException;

    int getNumColumns();

    /*
     * This will ensure that _any_ two Row implementations are equal based on the getObject(int position) method
     * implementation.
     */
    default boolean equalTo(Row other) throws SQLException {
        if (getNumColumns() != other.getNumColumns()) {
            return false;
        }

        for (int i = 0; i < getNumColumns(); i++) {
            Object thisPos = getObject(i);
            Object otherPos = other.getObject(i);
            if (thisPos == null) {
                if (otherPos != null) {
                    return false;
                }
            } else if (otherPos == null) {
                return false;
            } else if (!thisPos.equals(otherPos)) {
                return false;
            }
        }
        return true;
    }

    default String asString() throws SQLException {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < getNumColumns(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(getObject(i));
        }
        return sb.append(")").toString();
    }
}
