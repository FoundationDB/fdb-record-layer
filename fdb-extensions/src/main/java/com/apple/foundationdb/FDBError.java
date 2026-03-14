/*
 * ErrorCodes.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * FDB error codes (from https://apple.github.io/foundationdb/api-error-codes.html) and handy methods to
 * interpret them.
 */
@API(API.Status.UNSTABLE)
public enum FDBError {
    SUCCESS(0),
    OPERATION_FAILED(1000),
    TIMED_OUT(1004),
    TRANSACTION_TOO_OLD(1007),
    FUTURE_VERSION(1009),
    NOT_COMMITTED(1020),
    COMMIT_UNKNOWN_RESULT(1021),
    TRANSACTION_CANCELLED(1025),
    TRANSACTION_TIMED_OUT(1031),
    TOO_MANY_WATCHES(1032),
    WATCHES_DISABLED(1034),
    ACCESSED_UNREADABLE(1036),
    PROCESS_BEHIND(1037),
    DATABASE_LOCKED(1038),
    CLUSTER_VERSION_CHANGED(1039),
    EXTERNAL_CLIENT_ALREADY_LOADED(1040),
    PROXY_MEMORY_LIMIT_EXCEEDED(1042),
    BATCH_TRANSACTION_THROTTLED(1051),
    OPERATION_CANCELLED(1101),
    FUTURE_RELEASED(1102),
    TAG_THROTTLED(1213),
    PLATFORM_ERROR(1500),
    LARGE_ALLOC_FAILED(1501),
    PERFORMANCE_COUNTER_ERROR(1502),
    IO_ERROR(1510),
    FILE_NOT_FOUND(1511),
    BIND_FAILED(1512),
    FILE_NOT_READABLE(1513),
    FILE_NOT_WRITABLE(1514),
    NO_CLUSTER_FILE_FOUND(1515),
    FILE_TOO_LARGE(1516),
    CLIENT_INVALID_OPERATION(2000),
    COMMIT_READ_INCOMPLETE(2002),
    TEST_SPECIFICATION_INVALID(2003),
    KEY_OUTSIDE_LEGAL_RANGE(2004),
    INVERTED_RANGE(2005),
    INVALID_OPTION_VALUE(2006),
    INVALID_OPTION(2007),
    NETWORK_NOT_SETUP(2008),
    NETWORK_ALREADY_SETUP(2009),
    READ_VERSION_ALREADY_SET(2010),
    VERSION_INVALID(2011),
    RANGE_LIMITS_INVALID(2012),
    INVALID_DATABASE_NAME(2013),
    ATTRIBUTE_NOT_FOUND(2014),
    FUTURE_NOT_SET(2015),
    FUTURE_NOT_ERROR(2016),
    USED_DURING_COMMIT(2017),
    INVALID_MUTATION_TYPE(2018),
    TRANSACTION_INVALID_VERSION(2020),
    NO_COMMIT_VERSION(2021),
    ENVIRONMENT_VARIABLE_NETWORK_OPTION_FAILED(2022),
    TRANSACTION_READ_ONLY(2023),
    INCOMPATIBLE_PROTOCOL_VERSION(2100),
    TRANSACTION_TOO_LARGE(2101),
    KEY_TOO_LARGE(2102),
    VALUE_TOO_LARGE(2103),
    CONNECTION_STRING_INVALID(2104),
    ADDRESS_IN_USE(2105),
    INVALID_LOCAL_ADDRESS(2106),
    TLS_ERROR(2107),
    UNSUPPORTED_OPERATION(2108),
    API_VERSION_UNSET(2200),
    API_VERSION_ALREADY_SET(2201),
    API_VERSION_INVALID(2202),
    API_VERSION_NOT_SUPPORTED(2203),
    EXACT_MODE_WITHOUT_LIMITS(2210),
    UNKNOWN_ERROR(4000),
    INTERNAL_ERROR(4100),
    UNRECOGNIZED(-1)
    ;

    private final int code;

    private static final Map<Integer, FDBError> ERROR_BY_CODE;

    static {
        ImmutableMap.Builder<Integer, FDBError> builder = new ImmutableMap.Builder<>();
        for (FDBError error : FDBError.values()) {
            builder.put(error.code(), error);
        }
        ERROR_BY_CODE = builder.build();
    }

    FDBError(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    @Nonnull
    public static FDBError fromCode(int code) {
        final FDBError error = ERROR_BY_CODE.get(code);
        if (error == null) {
            return UNRECOGNIZED;
        }
        return error;
    }

    @Nonnull
    public static String toString(int errorCode) {
        final FDBError error = ERROR_BY_CODE.get(errorCode);
        if (error == null) {
            return "UNRECOGNIZED(" + errorCode + ")";
        }
        return error.name();
    }
}
