/*
 * KeySetProtobuf.java
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

package com.apple.foundationdb.relational.server.jdbc.v1;

import com.apple.foundationdb.relational.grpc.jdbc.v1.KeySet;
import com.apple.foundationdb.relational.grpc.jdbc.v1.KeySetValue;

import java.sql.SQLException;
import java.util.Map;

/**
 * Package-private utility converting protobuf KeySet to native KeySet.
 */
class KeySetProtobuf {
    /**
     * Make a native KeySet from a protobuf KeySet.
     */
    static com.apple.foundationdb.relational.api.KeySet map(KeySet protobufKeySet) throws SQLException {
        com.apple.foundationdb.relational.api.KeySet keySet = new com.apple.foundationdb.relational.api.KeySet();
        for (Map.Entry<String, KeySetValue> entry : protobufKeySet.getFieldsMap().entrySet()) {
            keySet.setKeyColumn(entry.getKey(),
                    entry.getValue().hasBytesValue() ? entry.getValue().getBytesValue() :
                            entry.getValue().hasLongValue() ? entry.getValue().getLongValue() :
                                    entry.getValue().hasStringValue() ? entry.getValue().getStringValue() : null);
        }
        return keySet;
    }
}
