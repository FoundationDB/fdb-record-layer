/*
 * ExceptionUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.RecordAlreadyExistsException;
import com.apple.foundationdb.record.provider.foundationdb.RecordContextNotActiveException;
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.UnableToPlanException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.google.common.base.VerifyException;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public final class ExceptionUtil {
    public static RelationalException toRelationalException(Throwable re) {
        if (re instanceof RelationalException) {
            return (RelationalException) re;
        } else if (re instanceof SQLException) {
            return new RelationalException(re.getMessage(), ErrorCode.get(((SQLException) re).getSQLState()), re);
        } else if (re instanceof RecordCoreException) {
            return recordCoreToRelationalException((RecordCoreException) re);
        } else if (re instanceof UncheckedRelationalException) {
            return ((UncheckedRelationalException) re).unwrap();
        } else if (re instanceof VerifyException) {
            return new RelationalException(re.getMessage(), ErrorCode.INTERNAL_ERROR, re);
        }
        return new RelationalException(ErrorCode.UNKNOWN, re);
    }

    private static RelationalException recordCoreToRelationalException(RecordCoreException re) {
        if (re.getCause() instanceof RelationalException) {
            return (RelationalException) re.getCause();
        }

        ErrorCode code = ErrorCode.UNKNOWN;
        if (re instanceof FDBExceptions.FDBStoreTransactionTimeoutException) {
            code = ErrorCode.TRANSACTION_TIMEOUT;
        } else if (re instanceof RecordContextNotActiveException || re.getCause() instanceof RecordContextNotActiveException) {
            code = ErrorCode.TRANSACTION_INACTIVE;
        } else if (re instanceof RecordDeserializationException || re.getCause() instanceof RecordDeserializationException) {
            code = ErrorCode.DESERIALIZATION_FAILURE;
        } else if (re instanceof RecordAlreadyExistsException || re.getCause() instanceof RecordAlreadyExistsException) {
            code = ErrorCode.UNIQUE_CONSTRAINT_VIOLATION;
        } else if (re instanceof MetaDataException) {
            //TODO(bfines) map this to specific error codes based on the violation
            code = ErrorCode.SYNTAX_OR_ACCESS_VIOLATION;
        } else if (re instanceof SemanticException) {
            code = translateErrorCode((SemanticException)re);
        } else if (re.getCause() instanceof SemanticException) {
            code = translateErrorCode((SemanticException)(re.getCause()));
        } else if (re instanceof UnableToPlanException) {
            code = ErrorCode.UNSUPPORTED_QUERY;
        }

        Map<String, Object> extraContext = re.getLogInfo();
        return new RelationalException(code, re).withContext(extraContext);
    }

    @Nonnull
    private static ErrorCode translateErrorCode(@Nonnull final SemanticException semanticException) {
        final var semanticErrorCode = semanticException.getErrorCode();
        switch (semanticErrorCode) {
            case INCOMPATIBLE_TYPE:
                return ErrorCode.CANNOT_CONVERT_TYPE;
            case FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES:
                return ErrorCode.INVALID_ARGUMENT_FOR_FUNCTION;
            case INVALID_CAST:
                return ErrorCode.INVALID_CAST;
            default:
                return ErrorCode.INTERNAL_ERROR;
        }
    }

    private ExceptionUtil() {
    }
}
