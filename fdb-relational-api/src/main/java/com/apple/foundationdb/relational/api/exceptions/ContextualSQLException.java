/*
 * ContextualSQLException.java
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

package com.apple.foundationdb.relational.api.exceptions;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An exception intended to carry over additional context within a SQLException.
 */
@SuppressWarnings("PMD.FormalParameterNamingConventions")
@API(API.Status.EXPERIMENTAL)
public class ContextualSQLException extends SQLException {
    private static final long serialVersionUID = 2135244094396331484L;
    @Nonnull
    private final transient Map<String, Object> context;

    public ContextualSQLException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
        this.context = Collections.emptyMap();
    }

    public ContextualSQLException(String reason, String SQLState) {
        super(reason, SQLState);
        this.context = Collections.emptyMap();
    }

    public ContextualSQLException(String reason) {
        super(reason);
        this.context = Collections.emptyMap();
    }

    public ContextualSQLException() {
        this.context = Collections.emptyMap();
    }

    public ContextualSQLException(Throwable cause) {
        super(cause);
        this.context = Collections.emptyMap();
    }

    public ContextualSQLException(String reason, Throwable cause) {
        super(reason, cause);
        this.context = Collections.emptyMap();
    }

    public ContextualSQLException(String reason, String sqlState, Throwable cause, @Nullable Map<String, Object> extraContext) {
        super(reason, sqlState, cause);
        if (extraContext != null) {
            //make a copy of the map for safety;
            this.context = Collections.unmodifiableMap(new HashMap<>(extraContext));
        } else {
            this.context = Collections.emptyMap();
        }
    }

    public ContextualSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
        this.context = Collections.emptyMap();
    }

    public Map<String, Object> getContext() {
        return context;
    }
}
