/*
 * TransactionalToken.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.server;

import com.apple.foundationdb.relational.api.RelationalConnection;

import java.sql.SQLException;

public class TransactionalToken {
    private RelationalConnection transactionalConnection;

    TransactionalToken(final RelationalConnection transactionalConnection) {
        this.transactionalConnection = transactionalConnection;
    }

    public RelationalConnection getConnection() {
        return transactionalConnection;
    }

    public boolean expired() {
        return transactionalConnection == null;
    }

    public void close() throws SQLException {
        if (!expired()) {
            transactionalConnection.close();
            transactionalConnection = null;
        }
    }
}
