/*
 * GrpcSQLException.java
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

package com.apple.foundationdb.relational.jdbc.grpc;

import com.apple.foundationdb.annotation.API;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

/**
 * Utility for serializing SQLExceptions for passing over grpc.
 * Includes encoding and decoding into and out of standard 'ErrorInfo' protobuf.
 */
@API(API.Status.EXPERIMENTAL)
public final class GrpcSQLExceptionUtil {
    private static final Logger logger = LogManager.getLogger(GrpcSQLExceptionUtil.class.getName());
    private static final String SQLEXCEPTION_ERRORINFO_DOMAIN = GrpcSQLExceptionUtil.class.getPackageName();
    private static final String SQLEXCEPTION_ERROR_CODE = "sqlExceptionErrorCode";
    private static final String SQLEXCEPTION_SQLSTATE = "sqlExceptionSQLState";
    private static final String SQLEXCEPTION_CAUSE = "sqlExceptionCause";
    private static final String SQLEXCEPTION_CAUSE_MESSAGE = "sqlExceptionCauseMessage";
    private static final String SQLEXCEPTION_STACK_TRACE = "sqlExceptionStackTrace";

    /**
     * When a sql exception, set the grpc Status code to UNKNOWN. The description for UNKNOWN in code.proto is not
     * helpful -- https://grpc.github.io/grpc/core/md_doc_statuscodes.html -- but if you read down the just-cited page,
     * you'll see UNKNOWN is used when server-side throws an exception which is what is going on here.
     * The INTERNAL code seems more appropriate until you read the description:
     * "Internal errors. This means that some invariants expected by the underlying system have been broken. This error
     * code is reserved for serious errors." (again from https://grpc.github.io/grpc/core/md_doc_statuscodes.html).
     */
    private static final int CODE_ON_SQLEXCEPTION = Code.UNKNOWN.getNumber();

    private GrpcSQLExceptionUtil() {
    }

    /**
     * Convert a {@link SQLException} to a 'standard' {@link com.google.rpc.ErrorInfo}
     * protobuf message.
     * @param sqlException Exception to flatten.
     * @return A {@link com.google.rpc.ErrorInfo} instance
     * filled out w/ message, error code, etc., gotten from `sqlException`.
     */
    static ErrorInfo map(SQLException sqlException) {
        ErrorInfo.Builder builder = ErrorInfo.newBuilder();
        builder.setDomain(SQLEXCEPTION_ERRORINFO_DOMAIN);
        // Google domains set a number in here for reason. See
        // https://github.com/googleapis/googleapis/blob/master/google/api/error_reason.proto
        // We'll do exception type for now.
        builder.setReason(sqlException.getClass().getName());
        builder.putMetadata(SQLEXCEPTION_ERROR_CODE, Integer.toString(sqlException.getErrorCode()));
        if (sqlException.getSQLState() != null) {
            builder.putMetadata(SQLEXCEPTION_SQLSTATE, sqlException.getSQLState());
        }
        if (sqlException.getCause() != null) {
            Throwable cause = sqlException.getCause();
            builder.putMetadata(SQLEXCEPTION_CAUSE, cause.getClass().getName());
            if (cause.getMessage() != null) {
                builder.putMetadata(SQLEXCEPTION_CAUSE_MESSAGE, cause.getMessage());
            }
        }
        if (sqlException.getStackTrace() != null) {
            builder.putMetadata(SQLEXCEPTION_STACK_TRACE, stacktraceToString(sqlException));
        }
        // TODO: Do we want to encode more? Do we want stack trace? For the original and cause exceptions?
        return builder.build();
    }

    /**
     * Create a grpc Status.
     * Serialize the passed sqlException into a created {@link Status} to pass
     * the exception across rpc. The client may not be java with a different means of handling
     * error so use 'standard' ErrorInfo protobuf conveying the sqlexception content.
     * @param sqlException The exception to serialize.
     * @return A {@link Status} instance populated by the content of sqlException.
     */
    public static Status create(SQLException sqlException) {
        // From https://techdozo.dev/getting-error-handling-right-in-grpc/
        // and https://www.vinsguru.com/grpc-error-handling/
        // and https://www.baeldung.com/grpcs-error-handling
        // and https://grpc.io/docs/guides/error/
        // and https://cloud.google.com/apis/design/errors#error_model
        return Status.newBuilder().setCode(CODE_ON_SQLEXCEPTION)
                .setMessage(sqlException.getMessage() != null ? sqlException.getMessage() : "")
                .addDetails(Any.pack(map(sqlException))).build();
    }

    /**
     * Convert the grpc RuntimeException to SQLException *IF* it was carrying one else returns null.
     * @param statusRuntimeException RuntimeException up from GRPC
     * @return A SQLException if the `statusRuntimeException` was carrying one else null.
     */
    @Nullable
    public static SQLException map(StatusRuntimeException statusRuntimeException) {
        Status status = StatusProto.fromThrowable(statusRuntimeException);
        return map(status);
    }

    @Nullable
    public static SQLException map(final Status status) {
        if (status.getCode() != CODE_ON_SQLEXCEPTION) {
            return null;
        }
        ErrorInfo errorInfo = null;
        for (Any any : status.getDetailsList()) {
            if (!any.is(ErrorInfo.class)) {
                logger.warn("Non-ErrorInfo Any type found: {} in {}", any.getClass(), status);
                continue;
            }
            try {
                if (errorInfo != null) {
                    logger.warn("More than one ErrorInfo found in {}, using first-found", status);
                    continue;
                }
                ErrorInfo ei = any.unpack(ErrorInfo.class);
                // Check the ErrorInfo found is one written by this class.
                if (ei.getDomain() != null && ei.getDomain().equals(SQLEXCEPTION_ERRORINFO_DOMAIN)) {
                    errorInfo = ei;
                }
            } catch (InvalidProtocolBufferException e) {
                // JUL won't let me log exception. TODO.
                if (logger.isWarnEnabled()) {
                    logger.warn(e.getMessage());
                }
            }
        }
        return errorInfo == null ? null : map(status.getMessage(), errorInfo);
    }

    /**
     * Fine exception cause and cause message in errorInfo metadata if present and return an instance or null if no
     * cause info found.
     * @param errorInfo metadata to mine for a cause.
     * @return A throwable recreated from metadata found in errorInfo or null if none present.
     */
    @Nullable
    private static Throwable getCause(ErrorInfo errorInfo) {
        String causeExceptionName = errorInfo.getMetadataOrDefault(SQLEXCEPTION_CAUSE, null);
        if (causeExceptionName == null) {
            return null;
        }
        String causeMessage = errorInfo.getMetadataOrDefault(SQLEXCEPTION_CAUSE_MESSAGE, null);
        try {
            Class<?> clazz = Class.forName(causeExceptionName);
            if (causeMessage == null) {
                return (Throwable) clazz.getDeclaredConstructor().newInstance();
            }
            Constructor<?> constructor = clazz.getDeclaredConstructor(String.class);
            return (Throwable) constructor.newInstance(causeMessage);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException |
                NoSuchMethodException e) {
            // Suspicious. We probably shouldn't be swallowing this exception
            if (logger.isTraceEnabled()) {
                logger.trace(e.getMessage());
            }
        }
        return null;
    }

    /**
     * Create a SQLException from the passed errorInfo and message.
     * @param message Message to use.
     * @param errorInfo protobuf instance to convert.
     * @return A client-side facsimile of the SQLException thrown server-side.
     * @see #map(SQLException)
     */
    private static SQLException map(String message, ErrorInfo errorInfo) {
        String sqlState = errorInfo.getMetadataOrDefault(SQLEXCEPTION_SQLSTATE, null);
        int vendorErrorCode = Integer.parseInt(errorInfo.getMetadataOrDefault(SQLEXCEPTION_ERROR_CODE, "-1"));
        Throwable cause = getCause(errorInfo);
        String stackTrace = errorInfo.getMetadataOrDefault(SQLEXCEPTION_STACK_TRACE, "");
        if (stackTrace != null && stackTrace.startsWith(message)) {
            message = stackTrace;
        }
        // Long-shot. Try to re-create the original exception type via reflection. If it works, great.
        // Otherwise, fall through to making a generic SQLException. This mechanism currently won't work for the relational
        // specializations on SQLException such as ContextualSQLException, not unless this grpc module depends on
        // api module or these classes get moved to fdb-relational-api... which we might want to do; as is, the relational
        // specializations are not on our classpath here. TODO. Also TODO, carry the map that ContextualSQLException adds.
        // errorInfo#getReason() is sqlexception type.
        String sqlExceptionName = errorInfo.getReason();
        if (sqlExceptionName != null && !sqlExceptionName.equals(SQLException.class.getName())) {
            try {
                Class<?> clazz = Class.forName(sqlExceptionName);
                Constructor<?> constructor =
                        clazz.getDeclaredConstructor(String.class, String.class, int.class, Throwable.class);
                if (constructor != null) {
                    return (SQLException) constructor.newInstance(message, sqlState, vendorErrorCode, cause);
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException |
                    NoSuchMethodException e) {
                // Just fall through to plain-old sqlexception.
                if (logger.isTraceEnabled() && e.getMessage() != null) {
                    logger.trace(e.getMessage());
                }
            }
            // Just fall through to plain-old sqlexception.
        }
        return new SQLException(message, sqlState, vendorErrorCode, cause);
    }

    public static String stacktraceToString(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        String str = stringWriter.toString();
        // Don't return massive headers. These exceptions end up serialized as grpc headers and there is a limit on
        // all headers of 8k. Pick arbitrary 4k size for exception.
        final int maxSize = 4 * 1024;
        return str.length() <= maxSize ? str : str.substring(0, maxSize);
    }
}
