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
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.RecordAlreadyExistsException;
import com.apple.foundationdb.record.provider.foundationdb.RecordContextNotActiveException;
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanComplexityException;
import com.apple.foundationdb.record.query.plan.UnsupportedQueryException;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.UnableToPlanException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.google.common.base.VerifyException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public final class ExceptionUtil {
    public static RelationalException toRelationalException(Throwable re) {
        if (re instanceof RelationalException relationalException) {
            return relationalException;
        } else if (re instanceof SQLException sqlException) {
            return new RelationalException(re.getMessage(), ErrorCode.get(sqlException.getSQLState()), re);
        } else if (re instanceof RecordCoreException recordCoreException) {
            return recordCoreToRelationalException(recordCoreException);
        } else if (re instanceof UncheckedRelationalException uncheckedException) {
            return uncheckedException.unwrap();
        } else if (re instanceof VerifyException) {
            // Verify.verify(false) throws a VerifyException with a null message; fall back to something more useful than a literal "null" reaching the client.
            final String message = Objects.requireNonNullElseGet(re.getMessage(), () -> "internal invariant violated (" + re.getClass().getSimpleName() + ")");
            return new RelationalException(message, ErrorCode.INTERNAL_ERROR, re);
        }
        return new RelationalException(ErrorCode.UNKNOWN, re);
    }

    private static RelationalException recordCoreToRelationalException(RecordCoreException re) {
        final Throwable cause = re.getCause();
        if (cause instanceof RelationalException relationalException) {
            return relationalException;
        }
        final ErrorCode code;
        if (re instanceof FDBExceptions.FDBStoreException storeException) {
            code = storeErrorCode(storeException);
        } else if (re instanceof RecordCoreRetriableTransactionException retriableException) {
            code = retriableErrorCode(retriableException);
        } else {
            code = otherErrorCode(re, cause);
        }

        Map<String, Object> extraContext = re.getLogInfo();
        final String message = Objects.requireNonNullElseGet(re.getMessage(), re::toString);
        return new RelationalException(message, code, re).withContext(extraContext);
    }

    /**
     * Error code for an FDB storage failure. Subclasses must be checked before {@link FDBExceptions.FDBStoreException}.
     */
    @Nonnull
    private static ErrorCode storeErrorCode(@Nonnull FDBExceptions.FDBStoreException re) {
        if (re instanceof FDBExceptions.FDBStoreTransactionTimeoutException) {
            return ErrorCode.TRANSACTION_TIMEOUT;
        } else if (re instanceof FDBExceptions.FDBStoreTransactionSizeException) {
            return ErrorCode.TRANSACTION_TOO_LARGE;
        } else if (re instanceof FDBExceptions.FDBStoreKeySizeException) {
            return ErrorCode.KEY_TOO_LARGE;
        } else if (re instanceof FDBExceptions.FDBStoreValueSizeException) {
            return ErrorCode.VALUE_TOO_LARGE;
        }
        return ErrorCode.STORAGE_ERROR;
    }

    /**
     * Error code for a retriable transaction failure. Subclasses must be checked before
     * {@link RecordCoreRetriableTransactionException}.
     */
    @Nonnull
    private static ErrorCode retriableErrorCode(@Nonnull RecordCoreRetriableTransactionException re) {
        if (re instanceof FDBExceptions.FDBStoreMaybeCommittedException) {
            return ErrorCode.TRANSACTION_RESOLUTION_UNKNOWN;
        } else if (re instanceof FDBExceptions.FDBStoreLockTakenException) {
            return ErrorCode.LOCK_NOT_AVAILABLE;
        } else if (re instanceof FDBExceptions.FDBStoreTransactionIsTooOldException) {
            return ErrorCode.TRANSACTION_TOO_OLD;
        } else if (re instanceof FDBExceptions.FDBStoreTransactionConflictException) {
            return ErrorCode.SERIALIZATION_FAILURE;
        }
        return ErrorCode.TRANSIENT_TRANSACTION_FAILURE;
    }

    @Nonnull
    private static ErrorCode otherErrorCode(@Nonnull RecordCoreException re, @Nullable Throwable cause) {
        if (re instanceof RecordContextNotActiveException || cause instanceof RecordContextNotActiveException) {
            return ErrorCode.TRANSACTION_INACTIVE;
        } else if (re instanceof RecordDeserializationException || cause instanceof RecordDeserializationException) {
            return ErrorCode.DESERIALIZATION_FAILURE;
        } else if (re instanceof RecordAlreadyExistsException || cause instanceof RecordAlreadyExistsException || re instanceof RecordIndexUniquenessViolation) {
            return ErrorCode.UNIQUE_CONSTRAINT_VIOLATION;
        } else if (re instanceof MetaDataException) {
            //TODO(bfines) map this to specific error codes based on the violation
            return ErrorCode.SYNTAX_OR_ACCESS_VIOLATION;
        } else if (re instanceof SemanticException semanticException) {
            return translateErrorCode(semanticException);
        } else if (cause instanceof SemanticException semanticException) {
            return translateErrorCode(semanticException);
        } else if (re instanceof RecordQueryPlanComplexityException) {
            return ErrorCode.STATEMENT_TOO_COMPLEX;
        } else if (re instanceof UnableToPlanException) {
            return ErrorCode.UNABLE_TO_PLAN;
        } else if (re instanceof UnsupportedQueryException) {
            return ErrorCode.UNSUPPORTED_QUERY;
        }
        return ErrorCode.UNKNOWN;
    }

    @Nonnull
    private static ErrorCode translateErrorCode(@Nonnull final SemanticException semanticException) {
        final var semanticErrorCode = semanticException.getErrorCode();

        switch (semanticErrorCode) {
            case INCOMPATIBLE_TYPE:
                return ErrorCode.CANNOT_CONVERT_TYPE;
            case FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES:
            case OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING:
                return ErrorCode.INVALID_ARGUMENT_FOR_FUNCTION;
            case ESCAPE_CHARACTER_CONFLICT:
                return ErrorCode.ESCAPE_CHARACTER_CONFLICT;
            case ESCAPE_CHAR_OF_LIKE_OPERATOR_IS_NOT_SINGLE_CHAR:
                return ErrorCode.INVALID_ESCAPE_CHARACTER;
            case INVALID_ESCAPE_SEQUENCE:
                return ErrorCode.INVALID_ESCAPE_SEQUENCE;
            case INVALID_CAST:
                return ErrorCode.INVALID_CAST;
            case COMPARISON_OF_INCOMPATIBLE_TYPES:
                return ErrorCode.DATATYPE_MISMATCH;
            case UNKNOWN_TYPE:
                return ErrorCode.UNKNOWN_TYPE;
            case UNSUPPORTED:
                return ErrorCode.UNSUPPORTED_OPERATION;
            default:
                return ErrorCode.INTERNAL_ERROR;
        }
    }

    private ExceptionUtil() {
    }
}
