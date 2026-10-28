/*
 * ExceptionUtilTest.java
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.RecordContextNotActiveException;
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanComplexityException;
import com.apple.foundationdb.record.query.plan.UnsupportedQueryException;
import com.apple.foundationdb.record.query.plan.cascades.UnableToPlanException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionUtilTest {

    @Test
    void detectsTransactionIsNoLongerActive() {
        RelationalException converted = ExceptionUtil.toRelationalException(new RecordContextNotActiveException("Transaction is no longer active"));
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, converted.getErrorCode());
    }

    @Test
    void detectsDeserializationFailure() {
        RelationalException converted = ExceptionUtil.toRelationalException(new RecordDeserializationException("Failed to deserialize record", new IllegalArgumentException()));
        Assertions.assertEquals(ErrorCode.DESERIALIZATION_FAILURE, converted.getErrorCode());
    }

    @Test
    void carriesRecordLayerContext() {
        RecordCoreException rece = new RecordCoreException("this is a test error message").addLogInfo("table", "foo");
        RelationalException converted = ExceptionUtil.toRelationalException(rece);
        Assertions.assertEquals(ErrorCode.UNKNOWN, converted.getErrorCode(), "Incorrect error code!");
        Map<String, Object> ctxMap = converted.getContext();
        Assertions.assertTrue(ctxMap.containsKey("table"), "context missing key!");
        Assertions.assertEquals("foo", ctxMap.get("table"), "Incorrect context value!");
    }

    static Stream<Arguments> toRelationalExceptionMapsToSpecificCode() {
        return Stream.of(
                Arguments.of(new FDBExceptions.FDBStoreTransactionTimeoutException(fdbException(FDBError.TRANSACTION_TIMED_OUT)), ErrorCode.TRANSACTION_TIMEOUT),
                Arguments.of(new FDBExceptions.FDBStoreTransactionSizeException(fdbException(FDBError.TRANSACTION_TOO_LARGE)), ErrorCode.TRANSACTION_TOO_LARGE),
                Arguments.of(new FDBExceptions.FDBStoreKeySizeException(fdbException(FDBError.KEY_TOO_LARGE)), ErrorCode.KEY_TOO_LARGE),
                Arguments.of(new FDBExceptions.FDBStoreValueSizeException(fdbException(FDBError.VALUE_TOO_LARGE)), ErrorCode.VALUE_TOO_LARGE),
                Arguments.of(new FDBExceptions.FDBStoreMaybeCommittedException(fdbException(FDBError.COMMIT_UNKNOWN_RESULT)), ErrorCode.TRANSACTION_RESULT_UNKNOWN),
                Arguments.of(new FDBExceptions.FDBStoreTransactionConflictException(fdbException(FDBError.NOT_COMMITTED)), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(new FDBExceptions.FDBStoreTransactionIsTooOldException(fdbException(FDBError.TRANSACTION_TOO_OLD)), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(new FDBExceptions.FDBStoreRetriableException(fdbException(FDBError.FUTURE_VERSION)), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(new FDBExceptions.FDBStoreLockTakenException("lock taken", new RuntimeException()), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(new FDBExceptions.FDBStoreException(fdbException(FDBError.INTERNAL_ERROR)), ErrorCode.STORAGE_ERROR),
                Arguments.of(new RecordQueryPlanComplexityException("too complex"), ErrorCode.PLAN_TOO_COMPLEX),
                Arguments.of(new UnableToPlanException("no plan"), ErrorCode.UNABLE_TO_PLAN),
                Arguments.of(new UnsupportedQueryException("unsupported"), ErrorCode.UNSUPPORTED_QUERY));
    }

    @ParameterizedTest
    @MethodSource
    void toRelationalExceptionMapsToSpecificCode(RecordCoreException exception, ErrorCode expected) {
        assertThat(ExceptionUtil.toRelationalException(exception).getErrorCode()).isEqualTo(expected);
    }

    private static FDBException fdbException(FDBError error) {
        return new FDBException(error.name(), error.code());
    }
}
