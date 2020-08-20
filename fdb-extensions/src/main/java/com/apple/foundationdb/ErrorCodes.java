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
@API(API.Status.MAINTAINED)
public class ErrorCodes {
    public static final int SUCCESS = 0;
    public static final int OPERATION_FAILED = 1000;
    public static final int TIMED_OUT = 1004;
    public static final int TRANSACTION_TOO_OLD = 1007;
    public static final int FUTURE_VERSION = 1009;
    public static final int NOT_COMMITTED = 1020;
    public static final int COMMIT_UNKNOWN_RESULT = 1021;
    public static final int TRANSACTION_CANCELLED = 1025;
    public static final int TRANSACTION_TIMED_OUT = 1031;
    public static final int TOO_MANY_WATCHES = 1032;
    public static final int WATCHES_DISABLED = 1034;
    public static final int ACCESSED_UNREADABLE = 1036;
    public static final int PROCESS_BEHIND = 1037;
    public static final int DATABASE_LOCKED = 1038;
    public static final int CLUSTER_VERSION_CHANGED = 1039;
    public static final int EXTERNAL_CLIENT_ALREADY_LOADED = 1040;
    public static final int PROXY_MEMORY_LIMIT_EXCEEDED = 1042;
    public static final int OPERATION_CANCELLED = 1101;
    public static final int FUTURE_RELEASED = 1102;
    public static final int PLATFORM_ERROR = 1500;
    public static final int LARGE_ALLOC_FAILED = 1501;
    public static final int PERFORMANCE_COUNTER_ERROR = 1502;
    public static final int IO_ERROR = 1510;
    public static final int FILE_NOT_FOUND = 1511;
    public static final int BIND_FAILED = 1512;
    public static final int FILE_NOT_READABLE = 1513;
    public static final int FILE_NOT_WRITABLE = 1514;
    public static final int NO_CLUSTER_FILE_FOUND = 1515;
    public static final int FILE_TOO_LARGE = 1516;
    public static final int CLIENT_INVALID_OPERATION = 2000;
    public static final int COMMIT_READ_INCOMPLETE = 2002;
    public static final int TEST_SPECIFICATION_INVALID = 2003;
    public static final int KEY_OUTSIDE_LEGAL_RANGE = 2004;
    public static final int INVERTED_RANGE = 2005;
    public static final int INVALID_OPTION_VALUE = 2006;
    public static final int INVALID_OPTION = 2007;
    public static final int NETWORK_NOT_SETUP = 2008;
    public static final int NETWORK_ALREADY_SETUP = 2009;
    public static final int READ_VERSION_ALREADY_SET = 2010;
    public static final int VERSION_INVALID = 2011;
    public static final int RANGE_LIMITS_INVALID = 2012;
    public static final int INVALID_DATABASE_NAME = 2013;
    public static final int ATTRIBUTE_NOT_FOUND = 2014;
    public static final int FUTURE_NOT_SET = 2015;
    public static final int FUTURE_NOT_ERROR = 2016;
    public static final int USED_DURING_COMMIT = 2017;
    public static final int INVALID_MUTATION_TYPE = 2018;
    public static final int TRANSACTION_INVALID_VERSION = 2020;
    public static final int NO_COMMIT_VERSION = 2021;
    public static final int ENVIRONMENT_VARIABLE_NETWORK_OPTION_FAILED = 2022;
    public static final int TRANSACTION_READ_ONLY = 2023;
    public static final int INCOMPATIBLE_PROTOCOL_VERSION = 2100;
    public static final int TRANSACTION_TOO_LARGE = 2101;
    public static final int KEY_TOO_LARGE = 2102;
    public static final int VALUE_TOO_LARGE = 2103;
    public static final int CONNECTION_STRING_INVALID = 2104;
    public static final int ADDRESS_IN_USE = 2105;
    public static final int INVALID_LOCAL_ADDRESS = 2106;
    public static final int TLS_ERROR = 2107;
    public static final int UNSUPPORTED_OPERATION = 2108;
    public static final int API_VERSION_UNSET = 2200;
    public static final int API_VERSION_ALREADY_SET = 2201;
    public static final int API_VERSION_INVALID = 2202;
    public static final int API_VERSION_NOT_SUPPORTED = 2203;
    public static final int EXACT_MODE_WITHOUT_LIMITS = 2210;
    public static final int UNKNOWN_ERROR = 4000;
    public static final int INTERNAL_ERROR = 4100;

    private static final Map<Integer, String> ERROR_TEXTMAP = new ImmutableMap.Builder<Integer, String>()
            .put(0, "SUCCESS")
            .put(1000, "OPERATION_FAILED")
            .put(1004, "TIMED_OUT")
            .put(1007, "TRANSACTION_TOO_OLD")
            .put(1009, "FUTURE_VERSION")
            .put(1020, "NOT_COMMITTED")
            .put(1021, "COMMIT_UNKNOWN_RESULT")
            .put(1025, "TRANSACTION_CANCELLED")
            .put(1031, "TRANSACTION_TIMED_OUT")
            .put(1032, "TOO_MANY_WATCHES")
            .put(1034, "WATCHES_DISABLED")
            .put(1036, "ACCESSED_UNREADABLE")
            .put(1037, "PROCESS_BEHIND")
            .put(1038, "DATABASE_LOCKED")
            .put(1039, "CLUSTER_VERSION_CHANGED")
            .put(1040, "EXTERNAL_CLIENT_ALREADY_LOADED")
            .put(1042, "PROXY_MEMORY_LIMIT_EXCEEDED")
            .put(1101, "OPERATION_CANCELLED")
            .put(1102, "FUTURE_RELEASED")
            .put(1500, "PLATFORM_ERROR")
            .put(1501, "LARGE_ALLOC_FAILED")
            .put(1502, "PERFORMANCE_COUNTER_ERROR")
            .put(1510, "IO_ERROR")
            .put(1511, "FILE_NOT_FOUND")
            .put(1512, "BIND_FAILED")
            .put(1513, "FILE_NOT_READABLE")
            .put(1514, "FILE_NOT_WRITABLE")
            .put(1515, "NO_CLUSTER_FILE_FOUND")
            .put(1516, "FILE_TOO_LARGE")
            .put(2000, "CLIENT_INVALID_OPERATION")
            .put(2002, "COMMIT_READ_INCOMPLETE")
            .put(2003, "TEST_SPECIFICATION_INVALID")
            .put(2004, "KEY_OUTSIDE_LEGAL_RANGE")
            .put(2005, "INVERTED_RANGE")
            .put(2006, "INVALID_OPTION_VALUE")
            .put(2007, "INVALID_OPTION")
            .put(2008, "NETWORK_NOT_SETUP")
            .put(2009, "NETWORK_ALREADY_SETUP")
            .put(2010, "READ_VERSION_ALREADY_SET")
            .put(2011, "VERSION_INVALID")
            .put(2012, "RANGE_LIMITS_INVALID")
            .put(2013, "INVALID_DATABASE_NAME")
            .put(2014, "ATTRIBUTE_NOT_FOUND")
            .put(2015, "FUTURE_NOT_SET")
            .put(2016, "FUTURE_NOT_ERROR")
            .put(2017, "USED_DURING_COMMIT")
            .put(2018, "INVALID_MUTATION_TYPE")
            .put(2020, "TRANSACTION_INVALID_VERSION")
            .put(2021, "NO_COMMIT_VERSION")
            .put(2022, "ENVIRONMENT_VARIABLE_NETWORK_OPTION_FAILED")
            .put(2023, "TRANSACTION_READ_ONLY")
            .put(2100, "INCOMPATIBLE_PROTOCOL_VERSION")
            .put(2101, "TRANSACTION_TOO_LARGE")
            .put(2102, "KEY_TOO_LARGE")
            .put(2103, "VALUE_TOO_LARGE")
            .put(2104, "CONNECTION_STRING_INVALID")
            .put(2105, "ADDRESS_IN_USE")
            .put(2106, "INVALID_LOCAL_ADDRESS")
            .put(2107, "TLS_ERROR")
            .put(2108, "UNSUPPORTED_OPERATION")
            .put(2200, "API_VERSION_UNSET")
            .put(2201, "API_VERSION_ALREADY_SET")
            .put(2202, "API_VERSION_INVALID")
            .put(2203, "API_VERSION_NOT_SUPPORTED")
            .put(2210, "EXACT_MODE_WITHOUT_LIMITS")
            .put(4000, "UNKNOWN_ERROR")
            .put(4100, "INTERNAL_ERROR")
            .build();

    @Nonnull
    public static String toString(int errorCode) {
        final String text = ERROR_TEXTMAP.get(errorCode);
        if (text == null) {
            return "UNKNOWN(" + errorCode + ")";
        }
        return text;
    }
}
