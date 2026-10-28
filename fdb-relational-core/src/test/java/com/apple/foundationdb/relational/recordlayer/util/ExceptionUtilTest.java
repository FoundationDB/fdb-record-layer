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
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.provider.foundationdb.APIVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.RecordContextNotActiveException;
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanComplexityException;
import com.apple.foundationdb.record.query.plan.UnsupportedQueryException;
import com.apple.foundationdb.record.query.plan.cascades.UnableToPlanException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.test.Tags;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Tag(Tags.RequiresFDB)
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

    @BeforeAll
    static void initFdbApi() {
        // wrapException evaluates FDB error predicates, which need the FDB API to be initialized
        FDBDatabaseFactory.instance().setAPIVersion(APIVersion.API_VERSION_7_1);
        FDBDatabaseFactory.instance().getDatabase(FDBTestEnvironment.randomClusterFile()).database();
    }

    static Stream<Arguments> toRelationalExceptionMapsToSpecificCode() {
        return Stream.of(
                Arguments.of(wrapFdbError(FDBError.TRANSACTION_TIMED_OUT), ErrorCode.TRANSACTION_TIMEOUT),
                Arguments.of(wrapFdbError(FDBError.TRANSACTION_TOO_LARGE), ErrorCode.TRANSACTION_TOO_LARGE),
                Arguments.of(wrapFdbError(FDBError.KEY_TOO_LARGE), ErrorCode.KEY_TOO_LARGE),
                Arguments.of(wrapFdbError(FDBError.VALUE_TOO_LARGE), ErrorCode.VALUE_TOO_LARGE),
                Arguments.of(wrapFdbError(FDBError.COMMIT_UNKNOWN_RESULT), ErrorCode.TRANSACTION_RESULT_UNKNOWN),
                Arguments.of(wrapFdbError(FDBError.CLUSTER_VERSION_CHANGED), ErrorCode.TRANSACTION_RESULT_UNKNOWN),
                Arguments.of(wrapFdbError(FDBError.NOT_COMMITTED), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(wrapFdbError(FDBError.TRANSACTION_TOO_OLD), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(wrapFdbError(FDBError.FUTURE_VERSION), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(wrapFdbError(FDBError.INTERNAL_ERROR), ErrorCode.STORAGE_ERROR),
                Arguments.of(named(new FDBExceptions.FDBStoreLockTakenException("lock taken", new RuntimeException())), ErrorCode.LOCK_NOT_AVAILABLE),
                Arguments.of(named(new RecordCoreRetriableTransactionException("retriable")), ErrorCode.SERIALIZATION_FAILURE),
                Arguments.of(named(new RecordQueryPlanComplexityException("too complex")), ErrorCode.PLAN_TOO_COMPLEX),
                Arguments.of(named(new UnableToPlanException("no plan")), ErrorCode.UNABLE_TO_PLAN),
                Arguments.of(named(new UnsupportedQueryException("unsupported")), ErrorCode.UNSUPPORTED_QUERY));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource
    void toRelationalExceptionMapsToSpecificCode(Throwable exception, ErrorCode expected) {
        assertThat(ExceptionUtil.toRelationalException(exception).getErrorCode()).isEqualTo(expected);
    }

    private static Named<Throwable> wrapFdbError(FDBError error) {
        return Named.of(error.name(), FDBExceptions.wrapException(new FDBException(error.name(), error.code())));
    }

    private static Named<Throwable> named(Throwable exception) {
        return Named.of(exception.getClass().getSimpleName(), exception);
    }
}
