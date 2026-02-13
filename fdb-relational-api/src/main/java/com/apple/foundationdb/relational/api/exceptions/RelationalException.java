/*
 * RelationalException.java
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public class RelationalException extends Exception {
    private static final long serialVersionUID = 1L;
    private final ErrorCode errorCode;

    /*
     * Various different projects make assumptions about the nature of error handling, and one of those
     * is that "context" for error messages is contained in a map which is carried along with the exception. This
     * makes it difficult (in the shorter term) for Relational to avoid doing the same, because a great deal
     * of logging and tooling is built around these assumptions. Therefore, we maintain this same
     * mapping structure.
     */
    private transient Map<String, Object> errorContext;

    public RelationalException(String message, ErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public RelationalException(String message, ErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public RelationalException(ErrorCode errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public RelationalException(SQLException sqle) {
        super(sqle.getMessage(), sqle);
        this.errorCode = ErrorCode.get(sqle.getSQLState());
    }

    public RelationalException(String message, SQLException sqle) {
        super(message, sqle);
        this.errorCode = ErrorCode.get(sqle.getSQLState());
    }

    public SQLException toSqlException() {
        if (getCause() instanceof SQLException) {
            return (SQLException) getCause();
        }
        return new ContextualSQLException(getMessage(), getErrorCode().getErrorCode(), this, errorContext);
    }

    /**
     * Add additional context to the Exception.
     *
     * This method is intended to carry "context" for easy logging purposes, it is <em>not</em> a replacement
     * for proper error codes <em>or</em> for effective error messages. Any information that is held within
     * this context should (within reason) also be contained within either the error message or the ErrorCode
     * fields.
     *
     * This method exists solely for the purposes of carrying a historical error handling logic forward, and
     * should not be relied upon for important information.
     *
     * @param ctxName the name of the additional context field.
     * @param ctxValue the value of the additional context field.
     * @return an exception holding the context.
     */
    public RelationalException addContext(String ctxName, Object ctxValue) {
        if (errorContext == null) {
            errorContext = new HashMap<>();
        }
        errorContext.put(ctxName, ctxValue);

        return this;
    }

    /**
     * Add additional context to the Exception.
     *
     * This method is intended to carry "context" for easy logging purposes, it is <em>not</em> a replacement
     * for proper error codes <em>or</em> for effective error messages. Any information that is held within
     * this context should (within reason) also be contained within either the error message or the ErrorCode
     * fields.
     *
     * This method exists solely for the purposes of carrying a historical error handling logic forward, and
     * should not be relied upon for important information.
     *
     * @param context additional context as a map.
     * @return an exception holding the context.
     */
    public RelationalException withContext(@Nonnull Map<String, Object> context) {
        if (errorContext == null) {
            errorContext = new HashMap<>();
        }
        errorContext.putAll(context);
        return this;
    }

    public UncheckedRelationalException toUncheckedWrappedException() {
        return new UncheckedRelationalException(this);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public Map<String, Object> getContext() {
        Map<String, Object> ret = errorContext;
        if (ret == null) {
            ret = Collections.emptyMap();
        } else {
            ret = Collections.unmodifiableMap(ret);
        }
        return ret;
    }
}
