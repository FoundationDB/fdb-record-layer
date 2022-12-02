/*
 * UNKNOWNStatusInterceptor.java
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

package com.apple.foundationdb.relational.server;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * From <a href="https://github.com/saturnism/grpc-by-example-java/tree/master">grpc-by-example</a>
 * Add info to UNKNOWN errors if w/o detail.
 */
class UNKNOWNStatusInterceptor implements ServerInterceptor {
    // Only Throwable classes listed here will be processed by the interceptor.
    // The interceptor will copy the cause's message & stacktrace into Status' description.
    private final Set<Class<? extends Throwable>> autowrapThrowables = new HashSet<>();

    public UNKNOWNStatusInterceptor(Collection<Class<? extends Throwable>> autowrapThrowables) {
        this.autowrapThrowables.addAll(autowrapThrowables);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            @Override
            public void sendMessage(RespT message) {
                super.sendMessage(message);
            }

            @Override
            public void close(Status status, Metadata trailers) {
                if (status.getCode() == Status.Code.UNKNOWN &&
                        status.getDescription() == null &&
                        status.getCause() != null &&
                        autowrapThrowables.contains(status.getCause().getClass())) {
                    Throwable e = status.getCause();
                    status = Status.INTERNAL
                            .withDescription(e.getMessage())
                            .augmentDescription(stacktraceToString(e));
                }
                super.close(status, trailers);
            }
        };

        return next.startCall(wrappedCall, headers);
    }

    private static String stacktraceToString(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        return stringWriter.toString();
    }
}
