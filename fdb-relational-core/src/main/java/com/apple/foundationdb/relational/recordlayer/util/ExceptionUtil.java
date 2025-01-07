/*
 * ExceptionUtil.java
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

package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.RecordAlreadyExistsException;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.base.VerifyException;

import java.sql.SQLException;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public class ExceptionUtil {
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
        } else if (re instanceof RecordCoreStorageException ||
                (re.getCause() != null && re.getCause() instanceof RecordCoreStorageException)) {
            // Async handling can make a RecordCoreStorageException with cause of RecordCoreStorageException
            // when 'Transaction is not active' exception.
            code = ErrorCode.TRANSACTION_INACTIVE;
        } else if (re instanceof RecordAlreadyExistsException || re.getCause() instanceof RecordAlreadyExistsException) {
            code = ErrorCode.UNIQUE_CONSTRAINT_VIOLATION;
        } else if (re instanceof MetaDataException) {
            //TODO(bfines) map this to specific error codes based on the violation
            code = ErrorCode.SYNTAX_OR_ACCESS_VIOLATION;
        } else if (re instanceof SemanticException) {
            if (((SemanticException) re).getErrorCode().equals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE)) {
                code = ErrorCode.CANNOT_CONVERT_TYPE;
            } else if (((SemanticException) re).getErrorCode().equals(SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES)) {
                code = ErrorCode.INVALID_ARGUMENT_FOR_FUNCTION;
            } else {
                code = ErrorCode.INTERNAL_ERROR;
            }
        } else if (re.getMessage().contains("Cascades planner could not plan query")) {
            code = ErrorCode.UNSUPPORTED_QUERY;
        }

        Map<String, Object> extraContext = re.getLogInfo();
        return new RelationalException(code, re).withContext(extraContext);
    }

    private ExceptionUtil() {
    }
}
