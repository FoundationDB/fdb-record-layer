/*
 * FDBExceptionsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CompletionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link FDBExceptions}.
 */
public class FDBExceptionsTest {
    private static final String EXCEPTION_CAUSE_MESSAGE = "the failure cause";
    private static final String PARENT_EXCEPTION_MESSAGE = "something failed asynchronously";

    @Test
    public void wrapRuntimeException() {
        Exception cause = createRuntimeException();
        final String methodName = "createRuntimeException";
        testWrappedStackTrace(cause, methodName);
    }

    @Test
    public void wrapCheckedException() {
        Exception cause = createCheckedException();
        final String methodName = "createCheckedException";
        testWrappedStackTrace(cause, methodName);
    }

    private void testWrappedStackTrace(Exception cause, String methodName) {
        Exception parent = createCompletionException(cause);

        final RuntimeException exception = FDBExceptions.wrapException(parent);
        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw));
        assertThat(sw.toString(), allOf(
                containsString(PARENT_EXCEPTION_MESSAGE),
                containsString(EXCEPTION_CAUSE_MESSAGE),
                containsString(this.getClass().getName() + ".createCompletionException"),
                containsString(this.getClass().getName() + "." + methodName)
        ));
    }

    private Exception createCompletionException(Exception cause) {
        return new CompletionException(PARENT_EXCEPTION_MESSAGE, cause);
    }

    private Exception createRuntimeException() {
        return new RuntimeException(EXCEPTION_CAUSE_MESSAGE);
    }

    private Exception createCheckedException() {
        return new Exception(EXCEPTION_CAUSE_MESSAGE);
    }

}
